if (!require("pacman")) install.packages("pacman")
pacman::p_load(DBI,RPostgres,RPostgreSQL,feather,dplyr,data.table,stringr)

# Play nice with the public database ! 
# Using feather to save downloads to disk, and only query the database if 
# a previous download has expired (is older than x number of days).
data_store_expiry_days <- 30
data_store_path <- file.path(Sys.getenv('HOME'),"R_TEMP")
if (!dir.exists(data_store_path)) {
  dir.create(data_store_path,showWarnings=FALSE)
}
#---------------------------
# Script Functions
#---------------------------
should_run_query <- function(target_file,expiry_days) {
  if (file.exists(target_file)){
    (Sys.time() - (expiry_days*60*60*24)) > 
      file.info(target_file)['mtime']$mtime
  } else {
    TRUE
  }
}
dbQuery_as_data_frame <- function (query,dbConnection){
  res <- con |> dbSendQuery(query)
  # fetch all results: 
  tmp_Results <- res |> dbFetch() 
  # clear restults and close connection
  res |> dbClearResult() 
  tmp_Results |> as.data.frame()
}
load_write_table <- function (
    tableName, 
    dbConnection, 
    expiry_days,
    data_directory
){
  fCancel <- FALSE
  objectName = tableName |> 
    stringr::str_replace_all("\\.","_") |>
    stringr::str_replace_all(" ","_")
  target_path <- data_directory |> 
    file.path(paste0(objectName,".feather"))
  if(
    should_run_query(
      target_path, 
      expiry_days = expiry_days
    )
  ){
    attempt <- try({
        base::assign(
          objectName, 
          paste0('SELECT * FROM ',tableName) |> 
          dbQuery_as_data_frame(dbConnection)
        )
      })
    if(class(attempt) == "try-error") {
      fCancel <- TRUE
    }
    if (fCancel == TRUE){
      #return nothing
      NULL
    } else {
      feather::write_feather(
        base::get(objectName), 
        target_path
      )
    }
  } else { #load from file
    base::assign(
      objectName, 
      file.path(data_directory,paste0(objectName,".feather")) |>
        feather::read_feather()
    )
  }
  if (fCancel == TRUE){
    # return nothing
    NULL
  } else {
    # return object
    base::get(objectName)
  }
}

#---------------------------
# End Script Functions
#---------------------------

# Connect to a specific postgres database i.e. Heroku 
# https://rnacentral.org/help/public-database 
con <- dbConnect(
  RPostgres::Postgres(),
  dbname = 'pfmegrnargs', 
  host = 'hh-pgsql-public.ebi.ac.uk',  # i.e. 'ec2-54-83-201-96.compute-1.amazonaws.com'
  port = 5432, # or any other port specified by your DBA 
  user = 'reader', 
  password = 'NWDMCE5xdipIjRrp' 
) 

# information_schema is not available...
dbRna_list_tables_all <- load_write_table (
  tableName = 'information_schema.tables',
  dbConnection =  con,
  expiry_days = data_store_expiry_days,
  data_directory = data_store_path
)

dbRna_list_tables <- dbRna_list_tables_all |> 
  dplyr::filter(table_schema == 'rnacen') 
  # could drop views with:  & table_type == 'BASE TABLE')

list_schema_table <-  stringr::str_c(
  dbRna_list_tables$table_schema,
  dbRna_list_tables$table_name,
  sep='.'
)

dbRna_list_dictionary_tables <- dbRna_list_tables_all |> 
  dplyr::filter(table_schema != 'rnacen') 

list_dictionary_schema_table <-  stringr::str_c(
  dbRna_list_dictionary_tables$table_schema,
  dbRna_list_dictionary_tables$table_name,
  sep='.'
)

# This loads the entire database into memory, you likely don't have enough RAM!
all_data <- lapply(list_schema_table, 
       FUN=load_write_table, 
       dbConnection =  con,
       expiry_days = data_store_expiry_days,
       data_directory = data_store_path
)

# This loads the entire data dictionary of the database into memory, RAM shouldn't be an issue
dictionary_data <- 
  lapply(list_dictionary_schema_table, 
         FUN=load_write_table, 
         dbConnection =  con,
         expiry_days = data_store_expiry_days,
         data_directory = data_store_path
  )
  

# 
# # if(
# #   should_run_query(
# #     file.path( data_store_path,"dbRna_list_tables.feather"),
# #     data_store_expiry_days
# #   )
# # ){
# #   # postgres://reader:NWDMCE5xdipIjRrp@hh-pgsql-public.ebi.ac.uk:5432/pfmegrnargs 
# #   dbRna_list_tables <- con |>  
# #     dbListTables() |> 
# #     data.table::as.data.table() # or  as_tibble()
# #   
# #   feather::write_feather(
# #     dbRna_list_tables, 
# #     file.path( data_store_path,"dbRna_list_tables.feather")
# #   )
# # } else { #load from file
# #   dbRna_list_tables <- 
# #     file.path(data_store_path,"dbRna_list_tables.feather") |>
# #     feather::read_feather()
# # }
# 
# if(
#   should_run_query(
#     file.path(data_store_path,"dbRna_table_has_field.feather"),
#     data_store_expiry_days
#   )
# ){
#   dbQueryAllFields <- function(dbTableName,dbConnection){
#     cat(paste0("Gettign list of fields for: ", dbTableName,'\n'))
#     dbConnection |> DBI::dbListFields(dbTableName) 
#   }
#   # build a listing of all fields for each table
#   dbRna_list_table_fields <- dbRna_list_tables$V1 |> 
#     lapply(FUN=dbQueryAllFields, dbConnection=con) 
#   
#   dbRna_possible_fields <-  dbRna_list_table_fields |>
#     unlist() |> as.data.frame() |> dplyr::distinct() |> setNames(c("field_names"))
# 
#   # populate an empty data.table
#   dbRna_table_has_field <- 
#     matrix(
#       rep(0L,(count(dbRna_possible_fields)$n)* count(dbRna_list_tables)$n),
#       ncol = count(dbRna_possible_fields)$n,
#       nrow = count(dbRna_list_tables)$n
#     ) |>
#     data.frame() |>
#     cbind.data.frame(
#       data.frame(
#         matrix(
#           rep("_",(count(dbRna_possible_fields)$n)* count(dbRna_list_tables)$n),
#           ncol = 1,
#           nrow = count(dbRna_list_tables)$n
#         ) 
#       )
#     ) |>
#     setNames(
#       c(
#         dbRna_possible_fields$field_names,
#         "schema_table_name"
#       )
#     )
#   
#   # Re-order variables (put schema_table_name first)
#   dbRna_table_has_field <-
#     dbRna_table_has_field |> 
#     dplyr::select(
#       c("schema_table_name",
#         dbRna_possible_fields$field_names |> base::sort()
#       )
#     )
#   
#   # fill the table name data
#   dbRna_table_has_field$schema_table_name <- dbRna_list_tables$V1
#   
#   # fill the rest of the data table
#   # [TODO] could re-write this for loop as an lapply, but this is fast enough
#   int_row <- 0L
#   for (field_list in dbRna_list_table_fields) {
#     int_row <- int_row + 1L
#     # update a row of the data.table in place
#     dbRna_table_has_field |>
#       data.table::set(i=int_row,j=field_list,value=1L)
#   }
#   # [End TODO]
#   rm(dbRna_possible_fields)
#   
#   dbRna_table_has_field |>
#     feather::write_feather(
#       file.path(data_store_path,"dbRna_table_has_field.feather")
#     )
# } else {
#   dbRna_table_has_field <- feather::read_feather(
#       file.path(data_store_path,"dbRna_table_has_field.feather")
#     )
# }
# 
# dbRna_other_objects <- RPostgres::dbListObjects(con)
# 
# # if speed is a problem, may need to re-write the REGEX in to a single statement.
# dbRna_other_objects$object_type <- 
#   dbRna_other_objects$table |> 
#   as.character() |> 
#   stringr::str_replace('(.*?)=','') |>
#   stringr::str_replace('=(.*)','') |>
#   stringr::str_replace_all(' ','') |>
#   stringr::str_replace_all('c\\(','') 
# 
# dbRna_other_objects <- dbRna_other_objects |> 
#   dplyr::filter(object_type != 'table')
# 
# dbRna_other_objects$object_name <- 
#   dbRna_other_objects$table |> 
#   as.character() |> 
#   stringr::str_replace('(.*?)=','') |>
#   stringr::str_replace('(.*?)=','') |>
#   stringr::str_replace_all('\\)','') |>
#   stringr::str_replace_all(' ','') 
# 
# # Using gsub instead of REGEX for stripping out the \" characters
# dbRna_other_objects$object_name <-gsub(
#   '\"', "", dbRna_other_objects$object_name, fixed = TRUE
# )
# 
# db_pg_catalog_indexes <- load_write_table(
#   "pg_catalog.pg_indexes",
#   dbConnection = con,
#   expiry_days = data_store_expiry_days,
#   data_directory = data_store_path
# )
# 
# 
# 
# db_pg_catalog_indexes <- 
#   "SELECT * FROM pg_catalog.pg_indexes" |> 
#   dbQuery_as_data_frame(conn)
# 
# # Number of distinct table names found in the indexes
# db_pg_catalog_indexes$tablename |> 
#   as.data.frame() |>
#   dplyr::distinct() |>
#   nrow()
# 
# # Number of distinct with a schema of rnacen
# cat (
#   "The number of tables that we can't find any index for in the rnacen schema are:",
#     nrow(dbRna_list_tables) - db_pg_catalog_indexes |> 
#     as.data.frame() |>
#     dplyr::filter(schemaname == 'rnacen') |>
#     dplyr::select(tablename) |>
#     dplyr::distinct() |>
#     nrow()
# )
# 
# db_pg_catalog_roles <- 
#   "SELECT * FROM pg_catalog.pg_authid" |> 
#   dbQuery_as_data_frame(conn)
# 
# cat(
#   "Max number of connection allowed by the 'reader' user is: ",
#   db_pg_catalog_roles |> 
#     as.data.frame() |> 
#     dplyr::filter(rolname == 'reader') |> 
#     dplyr::select(rolconnlimit) |> 
#     as.integer()
# )
# 
# # # nothing here
# # db_pg_catalog_largeobject_metadata <- 
# #   "SELECT * FROM pg_catalog.pg_largeobject_metadata" |> 
# #   dbQuery_as_data_frame(conn)
# 
# db_pg_catalog_pg_aggregate <- 
#   "SELECT * FROM pg_catalog.pg_aggregate" |> 
#   dbQuery_as_data_frame(conn)
# view(db_pg_catalog_pg_aggregate)
# 
# db_pg_catalog_pg_constraint <- 
#   "SELECT * FROM pg_catalog.pg_constraint" |> 
#   dbQuery_as_data_frame(conn)
# view(db_pg_catalog_pg_constraint)
# 
# db_pg_catalog_pg_database <- 
#   "SELECT * FROM pg_catalog.pg_database" |> 
#   dbQuery_as_data_frame(conn)
# view(db_pg_catalog_pg_database)
# 
# 
# db_pg_catalog_pg_db_role_setting <- 
#   "SELECT * FROM pg_catalog.pg_db_role_setting" |> 
#   dbQuery_as_data_frame(conn)
# view(db_pg_catalog_pg_db_role_setting)
# 
# 
# # #  Nothing Here
# # db_pg_catalog_pg_default_acl <- 
# #   "SELECT * FROM pg_catalog.pg_default_acl" |> 
# #   dbQuery_as_data_frame(conn)
# # view(db_pg_catalog_pg_default_acl)
# # 
# # db_pg_catalog_pg_event_trigger <-
# #   "SELECT * FROM pg_catalog.pg_event_trigger" |>
# #   dbQuery_as_data_frame(conn)
# # view(db_pg_catalog_pg_event_trigger)
# 
# db_pg_catalog_pg_extension <-
#   "SELECT * FROM pg_catalog.pg_extension" |>
#   dbQuery_as_data_frame(conn)
# view(db_pg_catalog_pg_extension)
# 
# 
# 
#  
# 
# ### look at some stats about all the tables that have a timestamp field
# contains_timestamp <- function(list_table_fields){
#   !is.na(match('timestamp',list_table_fields))
# }
# 
# table_has_create_field <- dbRna_list_table_fields[,1] |> 
#   lapply(FUN=contains_timestamp) 
# 
# tables_with_create_field <-table_has_create_field |>
#   cbind(dbRna_list_table_fields[,2]) |> 
#   as.data.frame() |>
#   dplyr::filter(table_has_create_field == TRUE) |>
#   dplyr::select(2)
# 
# 
# query_numeric_field_stats <- function(
#     schema_table_name,
#     dbConnection,
#     field_name_of_numeric="timestamp" # default field 
# ){
#   query <-     
#     paste0(
#       'SELECT ',
#       'MIN("',field_name_of_numeric,'") as "min_',field_name_of_numeric,', ',
#       'MAX("',field_name_of_numeric,'") as "max_',field_name_of_numeric,', ',
#       'count("',field_name_of_numeric,'") as "count_',field_name_of_numeric,', ',
#       'count(*) as count ',
#       'FROM "', schema_table_name, '" '
#       #, 'WHERE "', schema_table_name, '"."',field_name_of_numeric,'" IS NOT NULL'
#     )
#   query |> dbQuery_as_data_frame(dbConnection=dbConnection)
# }
# 
# db_timestamp_stats <- tables_with_create_field[,1] |>
#   lapply(FUN=query_numeric_field_stats, dbConnect=con) 
# 
# db_timestamp_stats |> skimr::skim()
# db_timestamp_stats |> summary()
# 
# # Example of how to fetch data a chunk at a time 
# res <- dbSendQuery(con, "SELECT * FROM rnc_database") 
# while(!dbHasCompleted(res)){ 
#   chunk <- dbFetch(res, n = 50) 
#   print(nrow(chunk)) 
# } 
# str(res) 
# # Clear the result
# dbClearResult(res) 


# Disconnect from the database 
dbDisconnect(con)
