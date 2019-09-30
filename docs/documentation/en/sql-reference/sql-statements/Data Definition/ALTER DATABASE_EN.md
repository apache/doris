# ALTER DATABASE
## description
This statement is used to set the properties of the specified database. (Administrators only)
Grammar:
1) Setting database data quota in B/K/KB/M/MB/G/GB/T/TB/P/PB
OTHER DATABASE dbu name SET DATA QUOTA quota;

2) Rename the database
ALTER DATABASE db_name RENAME new_db_name;

Explain:
After renaming the database, use REVOKE and GRANT commands to modify the corresponding user rights if necessary.

## example
1. Setting the specified database data quota
ALTER DATABASE example_db SET DATA QUOTA 10995116277760;
The above units are bytes, equivalent to
ALTER DATABASE example_db SET DATA QUOTA 10T;

ALTER DATABASE example_db SET DATA QUOTA 100G;

ALTER DATABASE example_db SET DATA QUOTA 200M;

2. Rename the database example_db to example_db2
ALTER DATABASE example_db RENAME example_db2;

## keyword
ALTER,DATABASE,RENAME

