---
{
    "title": "Doris On JDBC",
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

# JDBC External Table Of Doris

JDBC External Table Of Doris provides Doris to access external tables through the standard interface (JDBC) of database access. External tables save the tedious data import work, allowing Doris to have the ability to access various databases, and with the help of Doris's capabilities to solve data analysis problems with external tables:

1. Support various data sources to access Doris
2. Supports Doris's joint query with tables in various data sources for more complex analysis operations

This document mainly introduces how to use this function.


## Instructions

### Create JDBC external table in Doris

Specific table building syntax reference：[CREATE TABLE](../../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE.md)


#### 1. Create JDBC external table through JDBC_Resource
```sql
CREATE EXTERNAL RESOURCE jdbc_resource
properties (
    "type"="jdbc",
    "user"="root",
    "password"="123456",
    "jdbc_url"="jdbc:mysql://192.168.0.1:3306/test",
    "driver_url"="http://IP:port/mysql-connector-java-5.1.47.jar",
    "driver_class"="com.mysql.jdbc.Driver"
);
     
CREATE EXTERNAL TABLE `baseall_mysql` (
  `k1` tinyint(4) NULL,
  `k2` smallint(6) NULL,
  `k3` int(11) NULL,
  `k4` bigint(20) NULL,
  `k5` decimal(9, 3) NULL
) ENGINE=JDBC
PROPERTIES (
"resource" = "jdbc_resource",
"table" = "baseall",
"table_type"="mysql"
);
```
Parameter Description：

| Parameter        | Description |
| ---------------- | ----------------------------- |
| **type**         | "jdbc", Required flag of Resource Type 。|
| **user**         | Username used to access the external database。|
| **password**     | Password information corresponding to the user。|
| **jdbc_url**     | The URL protocol of JDBC, including database type, IP address, port number and database name, has different formats for different database protocols. for example mysql: "jdbc:mysql://127.0.0.1:3306/test"。|
| **driver_class** | The class name of the driver package for accessing the external database，for example mysql:com.mysql.jdbc.Driver. |
| **driver_url**   | The package driver URL used to download and access external databases。http://IP:port/mysql-connector-java-5.1.47.jar . During the local test of one BE, the jar package can be placed in the local path, "driver_url"=“ file:///home/disk1/pathTo/mysql-connector-java-5.1.47.jar ", In case of multiple BE test,  Must ensure that they have the same path information|
| **resource**     | The resource name that depends on when creating the external table in Doris corresponds to the name when creating the resource in the previous step.|
| **table**        | The table name mapped to the external database when creating the external table in Doris.|
| **table_type**   | When creating an appearance in Doris, the table comes from that database. for example mysql,postgresql,sqlserver,oracle.|

### Query usage

```
select * from mysql_table where k1 > 1000 and k3 ='term';
```
### Data write

After the JDBC external table is create in Doris, the data can be written directly by the `insert into` statement, the query results of Doris can be written to the JDBC external table, or the data can be imported from one JDBC table to another.

```
insert into mysql_table values(1, "doris");
insert into mysql_table select * from table;
```
#### Transaction


The data of Doris is written to the external table by a group of batch. If the import is interrupted, the data written before may need to be rolled back. Therefore, the JDBC external table supports transactions when data is written. Transaction support needs to be supported set by session variable: `enable_odbc_transcation`. ODBC transactions are also controlled by this variable.

```
set enable_odbc_transcation = true; 
```

Transactions ensure the atomicity of JDBC external table writing, but it will reduce the performance of data writing, so we can consider turning on the way as appropriate.

#### 1.Mysql

| Mysql Version | Mysql JDBC Driver Version       |
| ------------- | ------------------------------- |
| 8.0.30        | mysql-connector-java-5.1.47.jar |

#### 2.PostgreSQL
| PostgreSQL Version | PostgreSQL JDBC Driver Version |
| ------------------ | ------------------------------ |
| 14.5               | postgresql-42.5.0.jar          |

```sql
CREATE EXTERNAL RESOURCE jdbc_pg
properties (
    "type"="jdbc",
    "user"="postgres",
    "password"="123456",
    "jdbc_url"="jdbc:postgresql://127.0.0.1:5442/postgres?currentSchema=doris_test",
    "driver_url"="http://127.0.0.1:8881/postgresql-42.5.0.jar",
    "driver_class"="org.postgresql.Driver"
);

CREATE EXTERNAL TABLE `ext_pg` (
  `k1` int
) ENGINE=JDBC
PROPERTIES (
"resource" = "jdbc_pg",
"table" = "pg_tbl",
"table_type"="postgresql"
);
```

#### 3.SQLServer
| SQLserver Version | SQLserver JDBC Driver Version     |
| ------------- | -------------------------- |
| 2022          | mssql-jdbc-11.2.0.jre8.jar |

#### 4.oracle
| Oracle Version | Oracle JDBC Driver Version |
| ---------- | ------------------- |
| 11         | ojdbc6.jar          |

At present, only this version has been tested, and other versions will be added after testing


## Type matching

There are different data types among different databases. Here is a list of the matching between the types in each database and the data types in Doris.

### MySQL

|  MySQL   |  Doris   |
| :------: | :------: |
| BOOLEAN  | BOOLEAN  |
|   CHAR   |   CHAR   |
| VARCHAR  | VARCHAR  |
|   DATE   |   DATE   |
|  FLOAT   |  FLOAT   |
| TINYINT  | TINYINT  |
| SMALLINT | SMALLINT |
|   INT    |   INT    |
|  BIGINT  |  BIGINT  |
|  DOUBLE  |  DOUBLE  |
| DATETIME | DATETIME |
| DECIMAL  | DECIMAL  |


### PostgreSQL

|    PostgreSQL    |  Doris   |
| :--------------: | :------: |
|     BOOLEAN      | BOOLEAN  |
|       CHAR       |   CHAR   |
|     VARCHAR      | VARCHAR  |
|       DATE       |   DATE   |
|       REAL       |  FLOAT   |
|     SMALLINT     | SMALLINT |
|       INT        |   INT    |
|      BIGINT      |  BIGINT  |
| DOUBLE PRECISION |  DOUBLE  |
|    TIMESTAMP     | DATETIME |
|     DECIMAL      | DECIMAL  |

### Oracle

|  Oracle  |  Doris   |
| :------: | :------: |
|   CHAR   |   CHAR   |
| VARCHAR  | VARCHAR  |
|   DATE   | DATETIME |
| SMALLINT | SMALLINT |
|   INT    |   INT    |
|  NUMBER  | DECIMAL  |


### SQL server

| SQLServer |  Doris   |
| :-------: | :------: |
|    BIT    | BOOLEAN  |
|   CHAR    |   CHAR   |
|  VARCHAR  | VARCHAR  |
|   DATE    |   DATE   |
|   REAL    |  FLOAT   |
|  TINYINT  | TINYINT  |
| SMALLINT  | SMALLINT |
|    INT    |   INT    |
|  BIGINT   |  BIGINT  |
| DATETIME  | DATETIME |
|  DECIMAL  | DECIMAL  |


## Q&A

1. Besides mysql, Oracle, PostgreSQL, and SQL Server support more databases

At present, Doris only adapts to MySQL, Oracle, SQL Server, and PostgreSQL.  And planning to adapt other databases. In principle, any database that supports JDBC access can be accessed through the JDBC facade. If you need to access other appearances, you are welcome to modify the code and contribute to Doris.

1. Read the Emoji expression on the surface of MySQL, and there is garbled code

When Doris makes a JDBC appearance connection, because the default utf8 code in MySQL is utf8mb3, it cannot represent Emoji expressions that require 4-byte coding. Here, you need to set the code of the corresponding column to utf8mb4, set the server code to utf8mb4, and do not configure characterencoding in the JDBC URL when creating the MySQL appearance (this attribute does not support utf8mb4. If non utf8mb4 is configured, the expression cannot be written. Therefore, it should be left blank and not configured.)


```
Configuration items can be modified globally

Modify the my.ini file in the MySQL directory (the Linux system is the my.cnf file in the etc directory)
[client]
default-character-set=utf8mb4

[mysql]
Set MySQL default character set
default-character-set=utf8mb4

[mysqld]
Set up MySQL character set server
character-set-server=utf8mb4
collation-server=utf8mb4_unicode_ci
init_connect='SET NAMES utf8mb4

Modify the type of corresponding table and column
ALTER TABLE table_name MODIFY  colum_name  VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

ALTER TABLE table_name CHARSET=utf8mb4;

SET NAMES utf8mb4

```
