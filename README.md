# rna_central_data_analysis

# Notes:
 - This script discards any column from the postgres database that
 has a data type of "blob/raw"
 - Play nice with the public database ! 
 - Using feather to save downloads to disk, and only query the database if 
 a previous download has expired (is older than x number of days).
