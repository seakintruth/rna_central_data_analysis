#!/usr/bin/env Rscript
# Notes:
# - This script discards any column from the postgres database that
# has a data type of "blob/raw"
# - Play nice with the public database ! 
# - Using feather to save downloads to disk, and only query the database if 
# a previous download has expired (is older than x number of days).
if (!require("pacman")) install.packages("pacman")
pacman::p_load(DBI,RPostgres,RPostgreSQL,feather,dplyr,data.table,stringr)

# Script scoped variable, could be wrapped up as inputs to a main function...
data_schema_name <- 'rnacen'
data_store_expiry_days <- 30
data_store_path <- file.path(Sys.getenv('HOME'),"R_TEMP")

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

dbQuery_as_data_frame <- function (query,dbConnection){
  res <- con |> dbSendQuery(query)
  # fetch all results: 
  tmp_Results <- res |> dbFetch() 
  # clear restults and close connection
  res |> dbClearResult() 
  tmp_Results |> as.data.frame()
}

load_write_query <- function (
    queryName,
    query = NULL, 
    dbConnection, 
    expiry_days,
    data_directory
){
  # If no query is passed assuming queryName is a table or view & retrieve all
  if(is.null(query)) query <- paste0('SELECT * FROM ',queryName) 
  # replaces all punctuation that might exist in the queryName with an Underscore
  objectName = queryName |> 
    str_replace_all("[[:punct:]]", "_")
  cat(
    "\n querying object:",
    objectName,
    " with query: {",
    query,"}"
  )
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
          dbQuery_as_data_frame(dbConnection)
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
    names(df_named_value) <- ".table_name"
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
  

# This query comes from: https://stackoverflow.com/a/53208173
info_current_connection_count <- load_write_query(
    queryName="info_current_connection_count",
    query = 
      paste0(
        'select  * from ',
        '(select count(*) used from pg_stat_activity) q1, ',
        '(select setting::int res_for_super from pg_settings where name=$$superuser_reserved_connections$$) q2, ',
        '(select setting::int max_conn from pg_settings where name=$$max_connections$$) q3'
      ), 
    dbConnection=con, 
    expiry_days=-1, # this particular query always should expire!
    data_directory= data_store_path
)

cat(
  '\nThere are currently',
  as.integer(info_current_connection_count$used[1]),
  'open connections and',
  as.integer(info_current_connection_count$max_conn[1]) -
    as.integer(info_current_connection_count$used[1]),
  'connections available (with an additional',
  as.integer(info_current_connection_count$res_for_super[1]),
  'connections reserved)\n'
)  


# Disconnect from the database 
dbDisconnect(con)
