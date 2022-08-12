# rna_central_data_analysis
This collection of scripts queries the public postgresql database fount at [RNA Central](https://rnacentral.org/help/public-database)

The results of queries are stored in feather format to a directory named 'R_TEMP' in your %HOME% directory.



# Notes:
- Can be run as a script by 'Rscript' there are no short named arguments.
- This script discards any column from the postgres database that
has a data type of "blob/raw"
- Play nice with the public database ! 
- Using feather to save downloads to disk, and only query the database if 
 a previous download has expired (is older than x number of days).
 - Working with some large data sets here,  
 using data.table in place of data.frame through out
