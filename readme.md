### Dump data from Postgres Database only
1. Change config file
2. Set the migration_schema to "pull"
3. Set the lsit of table name you want to export
4. Run the code

### Insert data from CSV file to Clickhouse Database
1. Change config file
2. Create a directory named "dumps" and load all the csv files there
2. Set the migration_schema to "push"
3. Set the csv field in csv_files to the csv files name (ex:2024_03_02_table1.csv)
4. Set the target table name
5. Set the insert_batchsize per file (ex: 100000) 
6. Run the code


### Full migration from Postgres to Clickhouse
1. Change config file
2. Set the migration_schema to "full"
3. Set list of table name
4. Set the insert_batchsize per table (ex: 100000) 
5. Run the code


