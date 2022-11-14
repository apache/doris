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
```shell
├── bin
│   └── run.sh
├── conf
│   ├── doris_external_tables
│   ├── doris_tables
│   ├── env.conf
│   └── mysql_tables
└── lib
    ├── e_auto.sh
    ├── e_mysql_to_doris.sh
    ├── mysql_to_doris.sh
    ├── mysql_type_convert.sh
    └── sync_to_doris.sh
```

### configurations

All configuration files are in the conf directory.

#### env.conf
Fill in your doris and mysql information here.
```text
# doris env
fe_master_host=<your_fe_master_host>
fe_master_port=<your_fe_master_query_port>
doris_username=<your_doris_username>
doris_password=<your_doris_password>
doris_odbc_name=<your_doris_odbc_driver_name>

# mysql env
mysql_host=<your_mysql_host>
mysql_port=<your_mysql_port>
mysql_username=<your_mysql_username>
mysql_password=<your_mysql_password>
```

#### mysql_tables
Fill in the mysql table here, in the form of `database.table`.
```text
db1.table1
db1.table2
db2.table3
```

#### doris_tables
Fill in the doris olap table here, in the form of `database.table`.
```text
doris_db.table1
doris_db.table2
doris_db.table3
```

#### doris_external_tables
Fill in the doris external table here, in the form of `database.table`.
```text
doris_db.e_table1
doris_db.e_table2
doris_db.e_table3
```

### How to use
bin/run.sh is the startup shell script and this is options for the script:
```shell
Usage: run.sh [option]
    -e, --create-external-table: create doris external table
    -o, --create-olap-table: create doris olap table
    -i, --insert-data: insert data into doris olap table from doris external table
    -d, --drop-external-table: drop doris external table
    -a, --auto-external-table: create doris external table and auto check mysql schema change
    -h, --help: show usage
```

#### 1. Create doris odbc external table
use like this: 
```shell
sh bin/run.sh --create-external-table
```
or
```shell
sh bin/run.sh -e
```
then doris odbc external table has been created, and the table creation statement is generated in `result/mysql/e_mysql_to_doris.sql` file.

#### 2. Create doris olap table
use like this:
```shell
sh bin/run.sh --create-olap-table
```
or
```shell
sh bin/run.sh -o
```
then doris odbc olap table has been created, and the table creation statement is generated in `result/mysql/mysql_to_doris.sql` file.

#### 2. Create doris olap table and sync data from odbc external table
use like this:
```shell
sh bin/run.sh --create-olap-table --insert-data
```
or
```shell
sh bin/run.sh -o -i
```
then doris odbc olap table has been created, and the table creation statement is generated in `result/mysql/mysql_to_doris.sql` file, and the insert statement is generated in `result/mysql/sync_to_doris.sql` file.

The premise is that you have created the external table, if not, please create it first.

If you want to delete external table after data sync finished, add `--drop-external-table` or `-d` option.

#### 3. Create doris external table and sync schema change automatically
use like this:
```shell
sh bin/run.sh --auto-external-table
```
or
```shell
sh bin/run.sh -a
```

the program will run in the background, process id is saved in `e_auto.pid`.
