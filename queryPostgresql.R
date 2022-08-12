#!/usr/bin/env Rscript
# Notes:
# - Can be run as a script by 'Rscript' there are no short named arguments.
# - This script discards any column from the postgres database that
# has a data type of "blob/raw"
# - Play nice with the public database ! 
# - Using feather to save downloads to disk, and only query the database if 
# a previous download has expired (is older than x number of days).
# - Working with some large data sets here,  
# using data.table in place of data.frame through out
if (!require("pacman")) install.packages("pacman")
pacman::p_load(DBI,RPostgres,RPostgreSQL,feather,dplyr,data.table,stringr)

args = commandArgs(trailingOnly=TRUE)
if (length(args)!=0) {
  dfArgs <- matrix(args,ncol=2,byrow=TRUE) |>
    as.data.frame() 
  names(dfArgs) <- c("arg","value")
  dfArg_data_store_path <- dfArgs |>
    subset(arg == '--data_store_path')$value
  dfArg_data_store_expiry_days <- dfArgs |> 
    subset(arg == '--data_store_expiry_days')$value
  # WIP:not implemented yet:
  # dfArg_out_format <- subset(dfArgs,arg == '--out_format')$value
}

# 
# # example planned arguments
# ./rna_central_connection_count.R --out_format csv --data_store_path /data/data_store_rnacen --data_store_expiry_days 30
# chr [1:6] "--out_format" "csv" "--data_store_path" ...
# [1] "--out_format"             "csv"                     
# [3] "--data_store_path"        "/data/data_store_rnacen" 
# [5] "--data_store_expiry_days" "30"                      
# --out_format csv --data_store_path /data/data_store_rnacen --data_store_expiry_days 30

# Script scoped variable, could be wrapped up as inputs to a main function...
data_schema_name <- 'rnacen'

if(length(dfArg_data_store_expiry_days) == 1) {
  dfArg_data_store_expiry_days
} else {
  data_store_expiry_days <- 30
}

if(length(dfArg_data_store_path) == 1) {
  data_store_path <- dfArg_data_store_path
} else {
  data_store_path <- file.path(Sys.getenv('HOME'),"R_TEMP")
}

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

#---------------------------
# Script Functions
#---------------------------
dbQueryAllFields <- function(dbTableName,dbConnection){
  cat(paste0("Gettign list of fields for: ", dbTableName,'\n'))
  dbConnection |> DBI::dbListFields(dbTableName)
}

should_run_query <- function(target_file,expiry_days) {
  if (file.exists(target_file)){
    (Sys.time() - (expiry_days*60*60*24)) > 
      file.info(target_file)['mtime']$mtime
  } else {
    TRUE
  }
}

firstClass <- function(x){
  class(x)[[1]]
}

remove_invalid_fields <- function(x){
  x %>% 
    select_if(!sapply(., firstClass) %in% c("blob"))
}

dbQuery_as_data_table <- function (
    query,
    dbConnection,
    by_row_number = 0
){
  res <- con |>
    dbSendQuery(query)
  # fetch all results:
  if (by_row_number == 0){
    tmp_Results <- res |>
      dbFetch()|> 
      as.data.table()
  } else {
    while(!dbHasCompleted(res)){
      tmp_Results <- res |>
        dbFetch( n = by_row_number) |> 
        as.data.table()
      tmp_Results |> nrow() |> print()
    }
  }
  # clear results and close connection
  res |>
    dbClearResult()
  rm(res)
  tmp_Results 
}

load_write_query <- function (
    queryName,
    query = NULL, 
    dbConnection, 
    expiry_days,
    data_directory, 
    silent = FALSE
){
  # If no query is passed assuming queryName is a table or view & retrieve all
  if(is.null(query)) query <- paste0('SELECT * FROM ',queryName) 
  # replaces all punctuation that might exist in the queryName with an Underscore
  objectName = queryName |> 
    str_replace_all("[[:punct:]]", "_")
  if (!silent){
    cat(
      "\n querying object:",
      objectName,
      " with query: {",
      query,"}"
    )
  }
  fCancel <- FALSE
  target_path <- data_directory |> 
    file.path(paste0(objectName,".feather"))
  if(
    should_run_query(
      target_path, 
      expiry_days = expiry_days
    )
  ){
    attempt <- try(
      base::assign(
        objectName, 
        query |> 
          dbQuery_as_data_table(dbConnection)
      )
    )
    if(class(attempt)[1] == "try-error") {
      fCancel <- TRUE
    }
    if (fCancel == TRUE){
      #return nothing
      NULL
    } else {
      base::assign(
        objectName, 
        base::get(objectName) |>
          remove_invalid_fields()
      )
      feather::write_feather(
        base::get(objectName), 
        target_path
      )
    }
  } else { #load from file
    attempt <- try(
      base::assign(
        objectName, 
        target_path |>
          feather::read_feather()
      )
    )
    if(class(attempt)[1] == "try-error") {
      fCancel <- TRUE
      # delete the file that we attempted to load, and try again
      if (file.exists(target_path)) {
        file.remove(target_path)
      }
      if (!file.exists(target_path)) {
        load_write_query(
          queryName = queryName,
          dbConnection = dbConnection,
          expiry_days = expiry_days,
          data_directory = data_directory
        )
      }
    }
  }
  if (fCancel == TRUE){
    # return nothing
    NULL
  } else {
    # add a column that contains queryName (so we know the provinence of the data)
    df_named_value <- as_tibble(queryName)
    names(df_named_value) <- ".object_name"
    base::assign(
      objectName, 
      base::get(objectName) |> 
        tibble::add_column(
          df_named_value,
          .name_repair = "universal",
          .before = 1
        )
    )
    # return object
    base::get(objectName)
  }
}

#---------------------------
# End Script Functions
#---------------------------

if (!dir.exists(data_store_path)) {
  dir.create(data_store_path,showWarnings=FALSE)
}

# information_schema is not available...
dbRna_list_tables_all <- load_write_query (
  queryName = 'information_schema.tables',
  dbConnection =  con,
  expiry_days = data_store_expiry_days,
  data_directory = data_store_path
)

dbRna_list_tables <- dbRna_list_tables_all |> 
  dplyr::filter(table_schema == data_schema_name) 
# could drop views with:  & table_type == 'BASE TABLE')

list_schema_table <-  stringr::str_c(
  dbRna_list_tables$table_schema,
  dbRna_list_tables$table_name,
  sep='.'
)

dbRna_list_dictionary_tables <- dbRna_list_tables_all |> 
  dplyr::filter(table_schema != data_schema_name) 

list_dictionary_schema_table <-  stringr::str_c(
  dbRna_list_dictionary_tables$table_schema,
  dbRna_list_dictionary_tables$table_name,
  sep='.'
)

dictionary_data <-
  lapply(list_dictionary_schema_table,
         FUN=load_write_query,
         dbConnection =  con,
         expiry_days = data_store_expiry_days,
         data_directory = data_store_path
  )

is.integer64 <- function(x){
  class(x)=="integer64"
}

load_write_table_size <- function(table_name,dbConnection){
  query = paste0(
    "SELECT pg_size_pretty(pg_total_relation_size('",table_name,"')) ",
    "as pretty_size ,",
    "pg_total_relation_size('",table_name,"') as size ",
    "FROM  (select 1) as sole"
  )  
  dbQuery_as_data_table(
      query = query,
      dbConnection = dbConnection
    ) |>
    cbind(table_name) |>
    mutate_if(is.integer64, as.numeric)
}

#get a list of all tables and their sizes
info_db_table_size <- list_schema_table  |> 
  lapply(
    FUN=load_write_table_size,
    dbConnection = con
  )

df_info_db_table_size <- info_db_table_size |>
  unlist() |>
  matrix(ncol = 3, byrow = TRUE) |> 
  as.data.frame()

# Rename the df of table sizes
names(df_info_db_table_size) <- names(info_db_table_size[[1]])

# tmp <- df_info_db_table_size |> base::subset(size > 2^30)
df_info_db_table_size$size <- df_info_db_table_size$size |>
  as.numeric()

#filter list of table names to those that are > 1 GB
df_large_table <- df_info_db_table_size |> 
  dplyr::filter(size > 2^30) |> 
  dplyr::arrange(desc(size)) 
head(df_large_table)
tail(df_large_table)


#filter list of table names to those that are <= 1 GB
df_small_table <- df_info_db_table_size |> 
  dplyr::filter(size <= 2^30 & size > 0) |> 
  dplyr::arrange(desc(size)) 

head(df_small_table)
tail(df_small_table)

# Total database size (without pipes |>)
cat(
  round(
    sum(
      df_info_db_table_size$size
    )/2^30,
    2
  ), "GB"
)

# Total database size (with only pipes |>)
divide <- function (x,y){x/y}
df_info_db_table_size$size |> 
  sum() |>
  divide(2^30) |> # one GB
  round(2) |> 
  cat("GB")

# # # storing the entire database in memory is not a good idea
# # # (you will run out of RAM, nearing half a TB)
# # # the following will attempt to download every rnacen table
# # # all_data <-
# lapply(list_schema_table,
#        FUN=load_write_query,
#        dbConnection =  con,
#        expiry_days = data_store_expiry_days,
#        data_directory = data_store_path
# )

# load all small tables 
lapply(df_small_table$table_name,
       FUN=load_write_query,
       dbConnection =  con,
       expiry_days = data_store_expiry_days,
       data_directory = data_store_path
)

# # Large tables would need to be handled differently, 
# # fetch in chunks, and store in subsets of N rows, 
# # or custom filter for each of the 26 large tables.
# # df_large_table$table_name
# # load all large tables 
# lapply(df_large_table$table_name,
#        FUN=load_write_query,
#        dbConnection =  con,
#        expiry_days = data_store_expiry_days,
#        data_directory = data_store_path
# )
# 



####################################################
# the following still needs operational testing
####################################################
if(
   should_run_query(
     file.path(data_store_path,"dbRna_table_has_field.feather"),
     data_store_expiry_days
   )
 ){
   # build a listing of all fields for each table
   dbRna_list_table_fields <- list_schema_table |>
     lapply(FUN=dbQueryAllFields, dbConnection=con)
   
   dbRna_possible_fields <-  dbRna_list_table_fields |>
     unlist() |> as.data.frame() |> dplyr::distinct() |> setNames(c("field_names"))
   
   # populate an empty data.table
   dbRna_table_has_field <-
     matrix(
       rep(0L,(count(dbRna_possible_fields)$n)* count(dbRna_list_tables)$n),
       ncol = count(dbRna_possible_fields)$n,
       nrow = count(dbRna_list_tables)$n
     ) |>
     data.frame() |>
     cbind.data.frame(
       data.frame(
         matrix(
           rep("_",(count(dbRna_possible_fields)$n)* count(dbRna_list_tables)$n),
           ncol = 1,
           nrow = count(dbRna_list_tables)$n
         )
       )
     ) |>
     setNames(
       c(
         dbRna_possible_fields$field_names,
         "schema_table_name"
       )
     )
   
   # Re-order variables (put schema_table_name first)
   dbRna_table_has_field <-
     dbRna_table_has_field |>
     dplyr::select(
       c("schema_table_name",
         dbRna_possible_fields$field_names |> base::sort()
       )
     )
   
   # fill the table name data
   dbRna_table_has_field$schema_table_name <- dbRna_list_tables$V1
   
   # fill the rest of the data table
   # [TODO] could re-write this for loop as an lapply, but this is fast enough
   int_row <- 0L
   for (field_list in dbRna_list_table_fields) {
     int_row <- int_row + 1L
     # update a row of the data.table in place
     dbRna_table_has_field |>
       data.table::set(i=int_row,j=field_list,value=1L)
   }
   # [End TODO]
   rm(dbRna_possible_fields)
   
   
   dbRna_table_has_field |>
     feather::write_feather(
       file.path(data_store_path,"dbRna_table_has_field.feather")
     )
 } else {
   dbRna_table_has_field <- feather::read_feather(
     file.path(data_store_path,"dbRna_table_has_field.feather")
   )
 }
 
 dbRna_other_objects <- RPostgres::dbListObjects(con)
 
 # if speed is a problem, may need to re-write the REGEX in to a single statement.
 dbRna_other_objects$object_type <-
   dbRna_other_objects$table |>
   as.character() |>
   stringr::str_replace('(.*?)=','') |>
   stringr::str_replace('=(.*)','') |>
   stringr::str_replace_all(' ','') |>
   stringr::str_replace_all('c\\(','')
 
 dbRna_other_objects <- dbRna_other_objects |>
   dplyr::filter(object_type != 'table')
 
 dbRna_other_objects$object_name <-
   dbRna_other_objects$table |>
   as.character() |>
   stringr::str_replace('(.*?)=','') |>
   stringr::str_replace('(.*?)=','') |>
   stringr::str_replace_all('\\)','') |>
   stringr::str_replace_all(' ','')
 
 # Using gsub instead of REGEX for stripping out the \" characters
 dbRna_other_objects$object_name <-gsub(
   '\"', "", dbRna_other_objects$object_name, fixed = TRUE
 )
 
 ### look at some stats about all the tables that have a timestamp field
 contains_timestamp <- function(list_table_fields){
   !is.na(match('timestamp',list_table_fields))
 }
 
 table_has_create_field <- dbRna_list_table_fields[,1] |>
   lapply(FUN=contains_timestamp)
 
 tables_with_create_field <-table_has_create_field |>
   cbind(dbRna_list_table_fields[,2]) |>
   as.data.frame() |>
   dplyr::filter(table_has_create_field == TRUE) |>
   dplyr::select(2)
 
 
 query_numeric_field_stats <- function(
    schema_table_name,
    dbConnection,
    field_name_of_numeric="timestamp" # default field
 ){
   query <-
     paste0(
       'SELECT ',
       'MIN("',field_name_of_numeric,'") as "min_',field_name_of_numeric,', ',
       'MAX("',field_name_of_numeric,'") as "max_',field_name_of_numeric,', ',
       'count("',field_name_of_numeric,'") as "count_',field_name_of_numeric,', ',
       'count(*) as count ',
       'FROM "', schema_table_name, '" '
       #, 'WHERE "', schema_table_name, '"."',field_name_of_numeric,'" IS NOT NULL'
     )
   query |> dbQuery_as_data_table(dbConnection=dbConnection)
 }

 
 db_timestamp_stats <- tables_with_create_field[,1] |>
 lapply(FUN=query_numeric_field_stats, dbConnect=con)

db_timestamp_stats |> skimr::skim()
db_timestamp_stats |> summary()

# Disconnect from the database 
dbDisconnect(con)

object_memory_useage <- sort(sapply(ls(all.names = TRUE),function(x){object.size(get(x))}),
                             decreasing = TRUE)

cat("RAM Allocation total is",
    (sum(object_memory_useage)/2^20) |> round(2),
    "MB; by object:",
    (object_memory_useage/2^20) |> 
      round(2) |> paste0(" MB")
)
