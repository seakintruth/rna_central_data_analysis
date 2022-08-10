library(DBI)
# install.packages("feather")
library(feather)
library(dplyr)

# Defining Functions
dbListAllFields <- function(dbTableName,dbConnection){
  dbTableName |> print()
  dbConnection |> dbListFields(dbTableName) 
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

# postgres://reader:NWDMCE5xdipIjRrp@hh-pgsql-public.ebi.ac.uk:5432/pfmegrnargs 
list_tables <- con |>  
  dbListTables()

# build a matrix of all tables and thier columns
list_table_columns <- list_tables |> 
  lapply(FUN=dbListAllFields, dbConnection=con) |>
  cbind(list_tables) |>
  as.data.frame()

contains_timestamp <- function(list_table_fields){
 !is.na(match('timestamp',list_table_fields))
}

table_has_create_field <- list_table_columns[,1] |> 
  lapply(FUN=contains_timestamp) 

tables_with_create_field <-table_has_create_field |>
  cbind(list_table_columns[,2]) |> 
  as.data.frame() |>
  dplyr::filter(table_has_create_field == TRUE) |>
  dplyr::select(2)

query_timestamp_stats <- function(table_name,dbConnection){
  query <-     
    paste0(
      "SELECT ",
      "MIN(timestamp) as min, ",
      "MAX(timestamp) as max, ",
      "count(timestamp) as count ",
      "FROM ", table_name, " ",
      "WHERE timestamp IS NOT NULL"
    )
    
  res <- dbConnection |> dbSendQuery(query)
  
  # fetch all results: 
  table_timestamp_stats <- res |> dbFetch() 
  # clear restults and close connection
  res |> dbClearResult() 
  # return  
  table_timestamp_stats
}

db_timestamp_stats <- tables_with_create_field[,1] |>
  lapply(FUN=query_timestamp_stats, dbConnect=con)


# Example of how to fetch data a chunk at a time 
res <- dbSendQuery(con, "SELECT * FROM rnc_database") 
while(!dbHasCompleted(res)){ 
  chunk <- dbFetch(res, n = 50) 
  print(nrow(chunk)) 
} 
str(res) 
# Clear the result
dbClearResult(res) 


# Disconnect from the database 
dbDisconnect(con)
