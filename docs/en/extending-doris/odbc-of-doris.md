---
{
    "title": "Doris On ODBC",
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

The following parameters are accepted by ODBC external table:

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
* `[]`: The corresponding driver name in is the driver name. When creating an external table, the driver name of the external table should be consistent with that in the configuration file.
* `Driver=`: This should be setted in according to the actual be installation path of the driver. It is essentially the path of a dynamic library. Here, we need to ensure that the pre dependencies of the dynamic library are met.

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

## Database ODBC version correspondence

### Centos Operating System

The unixODBC versions used are: 2.3.1, Doris 0.15, centos 7.9, all of which are installed using yum.

#### 1.mysql

| Mysql version | Mysql ODBC version |
| ------------- | ------------------ |
| 8.0.27        | 8.0.27, 8.026      |
| 5.7.36        | 5.3.11, 5.3.13     |
| 5.6.51        | 5.3.11, 5.3.13     |
| 5.5.62        | 5.3.11, 5.3.13     |

#### 2. PostgreSQL

PostgreSQL's yum source rpm package address:

````
https://download.postgresql.org/pub/repos/yum/reporpms/EL-7-x86_64/pgdg-redhat-repo-latest.noarch.rpm
````

This contains all versions of PostgreSQL from 9.x to 14.x, including the corresponding ODBC version, which can be installed as needed.

| PostgreSQL Version | PostgreSQL ODBC Version      |
| ------------------ | ---------------------------- |
| 12.9               | postgresql12-odbc-13.02.0000 |
| 13.5               | postgresql13-odbc-13.02.0000 |
| 14.1               | postgresql14-odbc-13.02.0000 |
| 9.6.24             | postgresql96-odbc-13.02.0000 |
| 10.6               | postgresql10-odbc-13.02.0000 |
| 11.6               | postgresql11-odbc-13.02.0000 |

#### 3. Oracle

#### 

| Oracle版本                                                   | Oracle ODBC版本                            |
| ------------------------------------------------------------ | ------------------------------------------ |
| Oracle Database 11g Enterprise Edition Release 11.2.0.1.0 - 64bit Production | oracle-instantclient19.13-odbc-19.13.0.0.0 |
| Oracle Database 12c Standard Edition Release 12.2.0.1.0 - 64bit Production | oracle-instantclient19.13-odbc-19.13.0.0.0 |
| Oracle Database 18c Enterprise Edition Release 18.0.0.0.0 - Production | oracle-instantclient19.13-odbc-19.13.0.0.0 |
| Oracle Database 19c Enterprise Edition Release 19.0.0.0.0 - Production | oracle-instantclient19.13-odbc-19.13.0.0.0 |
| Oracle Database 21c Enterprise Edition Release 21.0.0.0.0 - Production | oracle-instantclient19.13-odbc-19.13.0.0.0 |

Oracle ODBC driver version download address:

```
https://download.oracle.com/otn_software/linux/instantclient/1913000/oracle-instantclient19.13-sqlplus-19.13.0.0.0-2.x86_64.rpm
https://download.oracle.com/otn_software/linux/instantclient/1913000/oracle-instantclient19.13-devel-19.13.0.0.0-2.x86_64.rpm
https://download.oracle.com/otn_software/linux/instantclient/1913000/oracle-instantclient19.13-odbc-19.13.0.0.0-2.x86_64.rpm
https://download.oracle.com/otn_software/linux/instantclient/1913000/oracle-instantclient19.13-basic-19.13.0.0.0-2.x86_64.rpm
```

## Ubuntu operating system

The unixODBC versions used are: 2.3.4, Doris 0.15, Ubuntu 20.04

#### 1. Mysql

| Mysql version | Mysql ODBC version |
| ------------- | ------------------ |
| 8.0.27        | 8.0.11, 5.3.13     |

Currently only tested this version, other versions will be added after testing

#### 2. PostgreSQL

| PostgreSQL Version | PostgreSQL ODBC Version |
| ------------------ | ----------------------- |
| 12.9               | psqlodbc-12.02.0000     |

For other versions, as long as you download the ODBC driver version that matches the major version of the database, there is no problem. This will continue to supplement the test results of other versions under the Ubuntu system.

#### 3. Oracle

The same as the Oracle database and ODBC correspondence of the Centos operating system, and the following method is used to install the rpm package under ubuntu.

In order to install rpm packages under ubuntu, we also need to install an alien, which is a tool that can convert rpm packages into deb installation packages

````
sudo apt-get install alien
````

Then execute the installation of the above four packages

````
sudo alien -i oracle-instantclient19.13-basic-19.13.0.0.0-2.x86_64.rpm
sudo alien -i oracle-instantclient19.13-devel-19.13.0.0.0-2.x86_64.rpm
sudo alien -i oracle-instantclient19.13-odbc-19.13.0.0.0-2.x86_64.rpm
sudo alien -i oracle-instantclient19.13-sqlplus-19.13.0.0.0-2.x86_64.rpm
````

## Data type mapping

There are different data types among different databases. Here, the types in each database and the data type matching in Doris are listed.

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

### SQLServer

|  SQLServer  | Doris  |             Alternation rules              |
| :------: | :----: | :-------------------------------: |
|  BOOLEAN  | BOOLEAN  |                         |
|   CHAR   |  CHAR  |            Only UTF8 encoding is supported            |
| VARCHAR | VARCHAR |       Only UTF8 encoding is supported      |
|   DATE/   |  DATE  |                                   |
|  REAL   |  FLOAT  |                                   |
|   TINYINT   | TINYINT |  |
|   SMALLINT  | SMALLINT |  |
|   INT  | INT |  |
|   BIGINT  | BIGINT |  |
|   FLOAT  | DOUBLE |  |
|   DATETIME/DATETIME2  | DATETIME |  |
|   DECIMAL/NUMERIC | DECIMAL |  |

## Q&A

1. Relationship with the original external table of MySQL?

After accessing the ODBC external table, the original way to access the MySQL external table will be gradually abandoned. If you have not used the MySQL external table before, it is recommended that the newly accessed MySQL tables use ODBC external table directly.
    
2. Besides MySQL, Oracle, SQLServer, PostgreSQL, can doris support more databases?

Currently, Doris only adapts to MySQL, Oracle, SQLServer, PostgreSQL. The adaptation of other databases is under planning. In principle, any database that supports ODBC access can be accessed through the ODBC external table. If you need to access other databases, you are welcome to modify the code and contribute to Doris.

3. When is it appropriate to use ODBC external tables?

    Generally, when the amount of external data is small and less than 100W, it can be accessed through ODBC external table. Since external table the cannot play the role of Doris in the storage engine and will bring additional network overhead, it is recommended to determine whether to access through external tables or import data into Doris according to the actual access delay requirements for queries.

4. Garbled code in Oracle access?

   Add the following parameters to the BE start up script: `export NLS_LANG=AMERICAN_AMERICA.AL32UTF8`R, Restart all be

5. ANSI Driver or Unicode Driver?

   Currently, ODBC supports both ANSI and Unicode driver forms, while Doris only supports Unicode driver. If you force the use of ANSI driver, the query results may be wrong.

6. Report Errors: `driver connect Err: 01000 [unixODBC][Driver Manager]Can't open lib 'Xxx' : file not found (0)`

   The driver for the corresponding data is not installed on each BE, or it is not installed in the be/conf/odbcinst.ini configure the correct path, or create the table with the driver namebe/conf/odbcinst.ini different

7. Report Errors: `Fail to convert odbc value 'PALO ' TO INT on column:'A'`

    Type conversion error, type of column `A` mapping of actual column type is different, needs to be modified

8. BE crash occurs when using old MySQL table and ODBC external driver at the same time


This is the compatibility problem between MySQL database ODBC driver and existing Doris depending on MySQL lib. The recommended solutions are as follows:

* Method 1: replace the old MySQL External Table by ODBC External Table, recompile BE close options **WITH_MySQL**

* Method 2: Do not use the latest 8. X MySQL ODBC driver replace with the 5. X MySQL ODBC driver

9. Push down the filtering condition

   The current ODBC appearance supports push down under filtering conditions. MySQL external table can support push down under all conditions. The functions of other databases are different from Doris, which will cause the push down query to fail. At present, except for the MySQL, other databases do not support push down of function calls. Whether Doris pushes down the required filter conditions can be confirmed by the 'explain' query statement.

10. Report Errors: `driver connect Err: xxx`

    Connection to the database fails. The` Err: part` represents the error of different database connection failures. This is usually a configuration problem. You should check whether the IP address, port or account password are mismatched.

    

