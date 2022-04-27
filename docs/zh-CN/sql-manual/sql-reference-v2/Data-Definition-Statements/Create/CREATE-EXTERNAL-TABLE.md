---
{
    "title": "CREATE-EXTERNAL-TABLE",
    "language": "zh-CN"
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

此语句用来创建外部表，具体语法参阅 [CREATE TABLE](./CREATE-TABLE.html)。

主要通过 ENGINE 类型来标识是哪种类型的外部表，目前可选 MYSQL、BROKER、HIVE、ICEBERG

1. 如果是 mysql，则需要在 properties 提供以下信息：

   ```sql
   PROPERTIES (
   	"host" = "mysql_server_host",
   	"port" = "mysql_server_port",
   	"user" = "your_user_name",
   	"password" = "your_password",
   	"database" = "database_name",
   	"table" = "table_name"
   )
   ```

   注意：

   - "table" 条目中的 "table_name" 是 mysql 中的真实表名。而 CREATE TABLE 语句中的 table_name 是该 mysql 表在 Doris 中的名字，可以不同。

   - 在 Doris 创建 mysql 表的目的是可以通过 Doris 访问 mysql 数据库。而 Doris 本身并不维护、存储任何 mysql 数据。

2. 如果是 broker，表示表的访问需要通过指定的broker, 需要在 properties 提供以下信息：

   ```sql
   PROPERTIES (
   	"broker_name" = "broker_name",
   	"path" = "file_path1[,file_path2]",
   	"column_separator" = "value_separator"
   	"line_delimiter" = "value_delimiter"
   )
   ```

   另外还需要提供Broker需要的Property信息，通过BROKER PROPERTIES来传递，例如HDFS需要传入

   ```sql
   BROKER PROPERTIES(
     "username" = "name",
     "password" = "password"
   )
   ```

   这个根据不同的Broker类型，需要传入的内容也不相同

   注意：

   - "path" 中如果有多个文件，用逗号[,]分割。如果文件名中包含逗号，那么使用 %2c 来替代。如果文件名中包含 %，使用 %25 代替
   - 现在文件内容格式支持CSV，支持GZ，BZ2，LZ4，LZO(LZOP) 压缩格式。

3. 如果是 hive，则需要在 properties 提供以下信息：

   ```sql
   PROPERTIES (
   	"database" = "hive_db_name",
   	"table" = "hive_table_name",
   	"hive.metastore.uris" = "thrift://127.0.0.1:9083"
   )
   ```

   其中 database 是 hive 表对应的库名字，table 是 hive 表的名字，hive.metastore.uris 是 hive metastore 服务地址。

4. 如果是 iceberg，则需要在 properties 中提供以下信息：

   ```sql
   PROPERTIES (
   	"iceberg.database" = "iceberg_db_name",
   	"iceberg.table" = "iceberg_table_name",
   	"iceberg.hive.metastore.uris" = "thrift://127.0.0.1:9083",
   	"iceberg.catalog.type" = "HIVE_CATALOG"
   )
   ```

   其中 database 是 Iceberg 对应的库名； 
   table 是 Iceberg 中对应的表名；
   hive.metastore.uris 是 hive metastore 服务地址； 
   catalog.type 默认为 HIVE_CATALOG。当前仅支持 HIVE_CATALOG，后续会支持更多 Iceberg catalog 类型。

### Example

1. 创建MYSQL外部表

   直接通过外表信息创建mysql表

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
   ```

   通过External Catalog Resource创建mysql表

   ```sql
   # 先创建Resource
   CREATE EXTERNAL RESOURCE "mysql_resource" 
   PROPERTIES
   (
     "type" = "odbc_catalog",
     "user" = "mysql_user",
     "password" = "mysql_passwd",
     "host" = "127.0.0.1",
      "port" = "8239"			
   );
   
   # 再通过Resource创建mysql外部表
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
   ```

2. 创建一个数据文件存储在HDFS上的 broker 外部表, 数据使用 "|" 分割，"\n" 换行

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
   ```

3. 创建一个hive外部表

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
   ```

4. 创建一个 Iceberg 外表

   ```sql
   CREATE TABLE example_db.t_iceberg 
   ENGINE=ICEBERG
   PROPERTIES (
   	"iceberg.database" = "iceberg_db",
   	"iceberg.table" = "iceberg_table",
   	"iceberg.hive.metastore.uris"  =  "thrift://127.0.0.1:9083",
   	"iceberg.catalog.type"  =  "HIVE_CATALOG"
   );
   ```


### Keywords

    CREATE, EXTERNAL, TABLE

### Best Practice

