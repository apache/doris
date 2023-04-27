---
{

    "title": "Mysql to Doris",
    "language": "en"

}
---

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

# Mysql to Doris

mysql to doris is mainly suitable for automating the creation of doris odbc tables, mainly implemented with shell scripts

## manual

mysql to doris code [here](https://github.com/apache/doris/tree/master/extension/mysql_to_doris)

### Directory Structure

```text
├── mysql_to_doris
│   ├── conf
│   │	├── doris.conf
│   │	├── mysql.conf
│   │	└── tables
│   ├── all_tables.sh
│   │
└── └── user_define_tables.sh   
```

1. all_tables.sh 

   This script mainly reads all the tables under the mysql specified library and automatically creates the Doris odbc external table

2. user_define_tables.sh 

   This script is mainly used for users to customize certain tables under the specified mysql library to automatically create Doris odbc external tables

3. conf

   Configuration file, `doris.conf` is mainly used to configure doris related, `mysql.conf` is mainly used to configure mysql related, `tables` is mainly used to configure user-defined mysql library tables

### full

1. Download using mysql to doris [here](https://github.com/apache/doris/tree/master/extension/mysql_to_doris)
2. Configuration related files
   
   ```shell
   #doris.conf
   master_host=
   master_port=
   doris_password=
   doris_odbc_name=''
   
   #mysql.conf
   mysql_host=
   mysql_password=
   ```
   
   | Configuration item | illustrate          |
   | -------------- | ----------------------- |
   | master_host    | Doris FE master node IP |
   | master_port    | Doris FE query_port port |
   | doris_password | Doris Password (default root user) |
   | doris_odbc_name | The name of mysql odbc in the odbcinst.ini configuration file under be/conf |
   | mysql_host     | Mysql IP |
   | mysql_password | Mysql Password (default root user) |
   
3. Execute the `all_tables.sh` script

```
sh all_tables.sh mysql_db_name doris_db_name
```
After successful execution, the files directory will be generated, and the directory will contain `tables` (table name) and `tables.sql` (doris odbc table creation statement)

### custom 

1. Modify the `conf/tables` file to add the name of the odbc table that needs to be created
2. To configure mysql and doris related information, refer to step 2 of full creation
3. Execute the `user_define_tables.sh` script

```
sh user_define_tables.sh mysql_db_name doris_db_name
```

After successful execution, the user_files directory will be generated, and the directory will contain `tables.sql` (doris odbc table creation statement)
