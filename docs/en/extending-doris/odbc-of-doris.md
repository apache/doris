---
{
    "title": "ODBC of Doris",
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


# ODBC External Table Of Doris

ODBC external table of Doris provides Doris access to external tables through the standard interface for database access (ODBC). The external table eliminates the tedious data import work and enables Doris to have the ability to access all kinds of databases. It solves the data analysis problem of external tables with Doris' OLAP capability.

1. Support various data sources to access Doris
2. Support Doris query with tables in various data sources to perform more complex analysis operations
3. Use insert into to write the query results executed by Doris to the external data source


This document mainly introduces the implementation principle and usage of this ODBC external table.

## Glossary

###  Noun in Doris

* FE: Frontend, the front-end node of Doris. Responsible for metadata management and request access.
* BE: Backend, Doris's back-end node. Responsible for query execution and data storage.

## How To Use

### Create ODBC External Table 

#### 1. Creating ODBC external table without resource

```
CREATE EXTERNAL TABLE `baseall_oracle` (
  `k1` decimal(9, 3) NOT NULL COMMENT "",
  `k2` char(10) NOT NULL COMMENT "",
  `k3` datetime NOT NULL COMMENT "",
  `k5` varchar(20) NOT NULL COMMENT "",
  `k6` double NOT NULL COMMENT ""
) ENGINE=ODBC
COMMENT "ODBC"
PROPERTIES (
"host" = "192.168.0.1",
"port" = "8086",
"user" = "test",
"password" = "test",
"database" = "test",
"table" = "baseall",
"driver" = "Oracle 19 ODBC driver",
"type" = "oracle"
);
```

#### 2. Creating ODBC external table by resource (recommended)
```
CREATE EXTERNAL RESOURCE `oracle_odbc`
PROPERTIES (
"type" = "odbc_catalog",
"host" = "192.168.0.1",
"port" = "8086",
"user" = "test",
"password" = "test",
"database" = "test",
"odbc_type" = "oracle",
"driver" = "Oracle 19 ODBC driver"
);
     
CREATE EXTERNAL TABLE `baseall_oracle` (
  `k1` decimal(9, 3) NOT NULL COMMENT "",
  `k2` char(10) NOT NULL COMMENT "",
  `k3` datetime NOT NULL COMMENT "",
  `k5` varchar(20) NOT NULL COMMENT "",
  `k6` double NOT NULL COMMENT ""
) ENGINE=ODBC
COMMENT "ODBC"
PROPERTIES (
"odbc_catalog_resource" = "oracle_odbc",
"database" = "test",
"table" = "baseall"
);
```

The following parameters are accepted by ODBC external table:：

Parameter | Description
---|---
**hosts** | IP address of external database
**driver** | The driver name of ODBC Driver, which needs to be/conf/odbcinst.ini. The driver names should be consistent.
**type** | The type of external database, currently supports Oracle, MySQL and PostgerSQL
**user** | The user name of database
**password** | password for the user


##### Installation and configuration of ODBC driver


Each database will provide ODBC access driver. Users can install the corresponding ODBC driver lib library according to the official recommendation of each database.

After installation of ODBC driver, find the path of the driver lib Library of the corresponding database. The modify be/conf/odbcinst.ini Configuration like:

```
[MySQL Driver]
Description     = ODBC for MySQL
Driver          = /usr/lib64/libmyodbc8w.so
FileUsage       = 1 
```
* `[]`:The corresponding driver name in is the driver name. When creating an external table, the driver name of the external table should be consistent with that in the configuration file.
* `Driver=`:  This should be setted in according to the actual be installation path of the driver. It is essentially the path of a dynamic library. Here, we need to ensure that the pre dependencies of the dynamic library are met.

**Remember, all BE nodes are required to have the same driver installed, the same installation path and the same be/conf/odbcinst.ini config.**


### Query usage

After the ODBC external table is create in Doris, it is no different from ordinary Doris tables except that the data model (rollup, pre aggregation, materialized view, etc.) in Doris cannot be used.

```
select * from oracle_table where k1 > 1000 and k3 ='term' or k4 like '%doris'
```

### Data write

After the ODBC external table is create in Doris, the data can be written directly by the `insert into` statement, the query results of Doris can be written to the ODBC external table, or the data can be imported from one ODBC table to another.

```
insert into oracle_table values(1, "doris");
insert into oracle_table select * from postgre_table;
```
#### Transaction


The data of Doris is written to the external table by a group of batch. If the import is interrupted, the data written before may need to be rolled back. Therefore, the ODBC external table supports transactions when data is written. Transaction support needs to be supported set by session variable: `enable_odbc_transcation`.

```
set enable_odbc_transcation = true; 
```

Transactions ensure the atomicity of ODBC external table writing, but it will reduce the performance of data writing ., so we can consider turning on the way as appropriate.

## Data type mapping

There are different data types among different database. Here, the types in each database and the data type matching in Doris are listed.

### MySQL 

|  MySQL  | Doris  |             Alternation rules              |
| :------: | :----: | :-------------------------------: |
|  BOOLEAN  | BOOLEAN  |                         |
|   CHAR   |  CHAR  |            Only UTF8 encoding is supported           |
| VARCHAR | VARCHAR |       Only UTF8 encoding is supported      |
|   DATE   |  DATE  |                                   |
|  FLOAT   |  FLOAT  |                                   |
|   TINYINT   | TINYINT |  |
|   SMALLINT  | SMALLINT |  |
|   INT  | INT |  |
|   BIGINT  | BIGINT |  |
|   DOUBLE  | DOUBLE |  |
|   DATE  | DATE |  |
|   DATETIME  | DATETIME |  |
|   DECIMAL  | DECIMAL |  |

### PostgreSQL

|  PostgreSQL  | Doris  |             Alternation rules              |
| :------: | :----: | :-------------------------------: |
|  BOOLEAN  | BOOLEAN  |                         |
|   CHAR   |  CHAR  |            Only UTF8 encoding is supported            |
| VARCHAR | VARCHAR |       Only UTF8 encoding is supported
|   DATE   |  DATE  |                                   |
|  REAL   |  FLOAT  |                                   |
|   SMALLINT  | SMALLINT |  |
|   INT  | INT |  |
|   BIGINT  | BIGINT |  |
|   DOUBLE  | DOUBLE |  |
|   TIMESTAMP  | DATETIME |  |
|   DECIMAL  | DECIMAL |  |

### Oracle                          

|  Oracle  | Doris  |            Alternation rules               |
| :------: | :----: | :-------------------------------: |
|  not support | BOOLEAN  |          Oracle can replace Boolean with number (1) |
|   CHAR   |  CHAR  |                       |
| VARCHAR | VARCHAR |              |
|   DATE   |  DATE  |                                   |
|  FLOAT   |  FLOAT  |                                   |
|  not support  | TINYINT | Oracle can be replaced by NUMBER |
|   SMALLINT  | SMALLINT |  |
|   INT  | INT |  |
|   not support  | BIGINT |  Oracle can be replaced by NUMBER |
|   not support  | DOUBLE | Oracle can be replaced by NUMBER |
|   DATE  | DATE |  |
|   DATETIME  | DATETIME |  |
|   NUMBER  | DECIMAL |  |

## Q&A

1. Relationship with the original external table of MySQL

After accessing the ODBC external table, the original way to access the MySQL external table will be gradually abandoned. If you have not used the MySQL external table before, it is recommended that the newly accessed MySQL tables use ODBC external table directly.
    
2. Besides MySQL and Oracle, can doris support more databases

Currently, Doris only adapts to MySQL and Oracle. The adaptation of other databases is under planning. In principle, any database that supports ODBC access can be accessed through the ODBC external table. If you need to access other database, you are welcome to modify the code and contribute to Doris.

3. When is it appropriate to use ODBC external tables.

    Generally, when the amount of external data is small and less than 100W. It can be accessed through ODBC external table. Since external table the can not play the role of Doris in the storage engine and will bring additional network overhead. it is recommended to determine whether to access through external tables or import data into Doris according to the actual access delay requirements for queries.

4. Garbled code in Oracle access

   Add the following parameters to the BE start up script：`export NLS_LANG=AMERICAN_AMERICA.AL32UTF8`， Restart all be

5. ANSI Driver or Unicode Driver ？

   Currently, ODBC supports both ANSI and Unicode driver forms, while Doris only supports Unicode driver. If you force the use of ANSI driver, the query results may be wrong.

6. Report Errors: `driver connect Err: 01000 [unixODBC][Driver Manager]Can't open lib 'Xxx' : file not found (0)`

   The driver for the corresponding data is not installed on each BE, or it is not installed in the be/conf/odbcinst.ini configure the correct path, or create the table with the driver namebe/conf/odbcinst.ini different

7. Report Errors: `fail to convert odbc value 'PALO ' TO INT`

    Type conversion error, type mapping of column needs to be modified

8. BE crash occurs when using old MySQL table and ODBC external driver at the same time


This is the compatibility problem between MySQL database ODBC driver and existing Doris depending on MySQL lib. The recommended solutions are as follows:

* Method 1: replace the old MySQL External Table by ODBC External Table, recompile BE close options **WITH_MySQL**

* Method 2: Do not use the latest 8. X MySQL ODBC driver replace with the 5. X MySQL ODBC driver

9. Push down the filtering condition

   The current ODBC appearance supports push down under filtering conditions。MySQL external table can support push down under all conditions. The functions of other databases are different from Doris, which will cause the push down query to fail. At present, except for the MySQL, other databases do not support push down of function calls. Whether Doris pushes down the required filter conditions can be confirmed by the 'explain' query statement.

10. Report Errors: `driver connect Err: xxx`

    Connection to the database fails. The` Err: part` represents the error of different database connection failures. This is usually a configuration problem. You should check whether the IP address, port or account password are mismatched.

    

