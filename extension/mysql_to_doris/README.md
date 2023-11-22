<!-- 
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Mysql to doris

This is an easy-to-use tool to help mysql users on using doris.

## Directory Structure
```text
├── bin
│   └── run.sh
├── conf
│   ├── doris_external_tables
│   ├── doris_tables
│   ├── env.conf
│   └── mysql_tables
└── lib
    ├── jdbc
    │   ├── create_jdbc_catalog.sh
    │   └── sync_to_doris.sh
    ├── e_auto.sh
    ├── e_mysql_to_doris.sh
    ├── get_tables.sh
    ├── mysql_to_doris.sh
    ├── mysql_type_convert.sh
    ├── sync_check.sh
    └── sync_to_doris.sh
```

## configurations

All configuration files are in the conf directory.

### env.conf
Fill in your doris and mysql information here.
```text
# doris env
fe_master_host=<your_fe_master_host>
fe_master_port=<your_fe_master_query_port>
doris_username=<your_doris_username>
doris_password=<your_doris_password>
doris_odbc_name=<your_doris_odbc_driver_name>
doris_jdbc_catalog=<jdbc_catalog_name>
doris_jdbc_default_db=<jdbc_default_database>
doris_jdcb_driver_url=<jdcb_driver_url>
doris_jdbc_driver_class=<jdbc_driver_class>

# mysql env
mysql_host=<your_mysql_host>
mysql_port=<your_mysql_port>
mysql_username=<your_mysql_username>
mysql_password=<your_mysql_password>
```

### mysql_tables
Fill in the mysql table here, in the form of `database.table`.
```text
db1.table1
db1.table2
db2.table3
```

### doris_tables
Fill in the doris olap table here, in the form of `database.table`.
```text
doris_db.table1
doris_db.table2
doris_db.table3
```

### doris_external_tables
Fill in the doris external table here, in the form of `database.table`.
```text
doris_db.e_table1
doris_db.e_table2
doris_db.e_table3
```

## How to use
bin/run.sh is the startup shell script and this is options for the script:
```shell
Usage: run.sh [option]
    -e, --create-external-table: create doris external table
    -o, --create-olap-table: create doris olap table
    -i, --insert-data: insert data into doris olap table from doris external table
    -d, --drop-external-table: drop doris external table
    -a, --auto-external-table: create doris external table and auto check mysql schema change
    --database: specify the database name to process all tables under the entire database, and separate multiple databases with ","
    -t, --type: specify external table type, valid options: ODBC(default), JDBC
    -h, --help: show usage
```

### 1. Create doris external table
use like this: 
```shell
sh bin/run.sh --create-external-table
```
or
```shell
sh bin/run.sh -e
```
then doris odbc external table has been created, and the table creation statement will be generated in `result/mysql/e_mysql_to_doris.sql` file.

If `--type` option is specified as `JDBC`, create jdbc catalog statement will be generated in `result/mysql/jdbc_catalog.sql` file.

### 2. Create doris olap table
use like this:
```shell
sh bin/run.sh --create-olap-table
```
or
```shell
sh bin/run.sh -o
```
then doris odbc olap table has been created, and the table creation statement will be generated in `result/mysql/mysql_to_doris.sql` file.

### 3. Create doris olap table and sync data from odbc external table
The premise is that you have created the external table, if not, please create it first.

use like this:
```shell
sh bin/run.sh --create-olap-table --insert-data
```
or
```shell
sh bin/run.sh -o -i
```
then doris odbc olap table has been created, and the table creation statement will be generated in `result/mysql/mysql_to_doris.sql` file, and the insert statement is generated in `result/mysql/sync_to_doris.sql` file.

#### Sync result check
After the data is synchronized, the task of checking the synchronization results will be executed to compare the data volume of the olap table and mysql table, and the checking results will be saved in the `result/mysql/sync_check` file.

#### Drop external table
If you want to drop odbc external table after data sync finished, add `--drop-external-table` or `-d` option.

use like this:
```shell
sh bin/run.sh --drop-external-table
```
or
```shell
sh bin/run.sh -d
```

this option is valid for `ODBC` type.

### 4. Create doris external table and sync schema change automatically
use like this:
```shell
sh bin/run.sh --auto-external-table
```
or
```shell
sh bin/run.sh -a
```

the program will run in the background, process id is saved in `e_auto.pid`, and this option is valid for `ODBC` type.

### Process by specifying the database
If you have a large number of tables and do not need to customize the doris table name, you can specify the database name to be processed through the `--databases` option without manual configuration.

use like this:
```shell
# single database
sh bin/run.sh --database db1
```
or
```shell
# multiple databases 
sh bin/run.sh --database db1,db2,db3
```

With this option, the program will automatically obtain all the tables under the specified mysql database, and generate the configuration of `mysql_tables`, `doris_tables` and `doris_external_tables`.

**Please note that this option needs to be used in conjunction with other options.**
