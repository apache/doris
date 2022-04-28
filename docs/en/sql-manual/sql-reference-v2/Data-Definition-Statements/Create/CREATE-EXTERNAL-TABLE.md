---
{
    "title": "CREATE-EXTERNAL-TABLE",
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

## CREATE-EXTERNAL-TABLE

### Name

CREATE EXTERNAL TABLE

### Description

This statement is used to create an external table, see [CREATE TABLE](./CREATE-TABLE.html) for the specific syntax.

Which type of external table is mainly identified by the ENGINE type, currently MYSQL, BROKER, HIVE, ICEBERG are optional

1. If it is mysql, you need to provide the following information in properties:

   ```sql
   PROPERTIES (
   "host" = "mysql_server_host",
   "port" = "mysql_server_port",
   "user" = "your_user_name",
   "password" = "your_password",
   "database" = "database_name",
   "table" = "table_name"
   )
   ````

   Notice:

   - "table_name" in "table" entry is the real table name in mysql. The table_name in the CREATE TABLE statement is the name of the mysql table in Doris, which can be different.

   - The purpose of creating a mysql table in Doris is to access the mysql database through Doris. Doris itself does not maintain or store any mysql data.

2. If it is a broker, it means that the access to the table needs to pass through the specified broker, and the following information needs to be provided in properties:

   ```sql
   PROPERTIES (
   "broker_name" = "broker_name",
   "path" = "file_path1[,file_path2]",
   "column_separator" = "value_separator"
   "line_delimiter" = "value_delimiter"
   )
   ````

   In addition, you need to provide the Property information required by the Broker, and pass it through the BROKER PROPERTIES, for example, HDFS needs to pass in

   ```sql
   BROKER PROPERTIES(
     "username" = "name",
     "password" = "password"
   )
   ````

   According to different Broker types, the content that needs to be passed in is also different.

   Notice:

   - If there are multiple files in "path", separate them with comma [,]. If the filename contains a comma, use %2c instead. If the filename contains %, use %25 instead
   - Now the file content format supports CSV, and supports GZ, BZ2, LZ4, LZO (LZOP) compression formats.

3. If it is hive, you need to provide the following information in properties:

   ```sql
   PROPERTIES (
   "database" = "hive_db_name",
   "table" = "hive_table_name",
   "hive.metastore.uris" = "thrift://127.0.0.1:9083"
   )
   ````

   Where database is the name of the library corresponding to the hive table, table is the name of the hive table, and hive.metastore.uris is the address of the hive metastore service.

4. In case of iceberg, you need to provide the following information in properties:

   ```sql
   PROPERTIES (
   "iceberg.database" = "iceberg_db_name",
   "iceberg.table" = "iceberg_table_name",
   "iceberg.hive.metastore.uris" = "thrift://127.0.0.1:9083",
   "iceberg.catalog.type" = "HIVE_CATALOG"
   )
   ````

   Where database is the library name corresponding to Iceberg;
   table is the corresponding table name in Iceberg;
   hive.metastore.uris is the hive metastore service address;
   catalog.type defaults to HIVE_CATALOG. Currently only HIVE_CATALOG is supported, more Iceberg catalog types will be supported in the future.

### Example

1. Create a MYSQL external table

   Create mysql table directly from outer table information

   ```sql
   CREATE EXTERNAL TABLE example_db.table_mysql
   (
   k1 DATE,
   k2 INT,
   k3 SMALLINT,
   k4 VARCHAR(2048),
   k5 DATETIME
   )
   ENGINE=mysql
   PROPERTIES
   (
   "host" = "127.0.0.1",
   "port" = "8239",
   "user" = "mysql_user",
   "password" = "mysql_passwd",
   "database" = "mysql_db_test",
   "table" = "mysql_table_test"
   )
   ````

   Create mysql table through External Catalog Resource

   ```sql
   # Create Resource first
   CREATE EXTERNAL RESOURCE "mysql_resource"
   PROPERTIES
   (
     "type" = "odbc_catalog",
     "user" = "mysql_user",
     "password" = "mysql_passwd",
     "host" = "127.0.0.1",
      "port" = "8239"
   );
   
   # Then create mysql external table through Resource
   CREATE EXTERNAL TABLE example_db.table_mysql
   (
   k1 DATE,
   k2 INT,
   k3 SMALLINT,
   k4 VARCHAR(2048),
   k5 DATETIME
   )
   ENGINE=mysql
   PROPERTIES
   (
   "odbc_catalog_resource" = "mysql_resource",
   "database" = "mysql_db_test",
   "table" = "mysql_table_test"
   )
   ````

2. Create a broker external table with data files stored on HDFS, the data is split with "|", and "\n" is newline

   ```sql
   CREATE EXTERNAL TABLE example_db.table_broker (
   k1 DATE,
   k2 INT,
   k3 SMALLINT,
   k4 VARCHAR(2048),
   k5 DATETIME
   )
   ENGINE=broker
   PROPERTIES (
   "broker_name" = "hdfs",
   "path" = "hdfs://hdfs_host:hdfs_port/data1,hdfs://hdfs_host:hdfs_port/data2,hdfs://hdfs_host:hdfs_port/data3%2c4",
   "column_separator" = "|",
   "line_delimiter" = "\n"
   )
   BROKER PROPERTIES (
   "username" = "hdfs_user",
   "password" = "hdfs_password"
   )
   ````

3. Create a hive external table

   ```sql
   CREATE TABLE example_db.table_hive
   (
     k1 TINYINT,
     k2 VARCHAR(50),
     v INT
   )
   ENGINE=hive
   PROPERTIES
   (
     "database" = "hive_db_name",
     "table" = "hive_table_name",
     "hive.metastore.uris" = "thrift://127.0.0.1:9083"
   );
   ````

4. Create an Iceberg skin

   ```sql
   CREATE TABLE example_db.t_iceberg
   ENGINE=ICEBERG
   PROPERTIES (
   "iceberg.database" = "iceberg_db",
   "iceberg.table" = "iceberg_table",
   "iceberg.hive.metastore.uris" = "thrift://127.0.0.1:9083",
   "iceberg.catalog.type" = "HIVE_CATALOG"
   );
   ````


### Keywords

    CREATE, EXTERNAL, TABLE

### Best Practice

