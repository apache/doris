---
{
    "title": "JDBC External Table",
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

# JDBC External Table

<version deprecated="1.2.2">

Please use [JDBC Catalog](https://doris.apache.org/docs/dev/lakehouse/multi-catalog/jdbc/) to access JDBC data sources, this function will no longer be maintained after version 1.2.2.

</version>

<version since="1.2.0">

By creating JDBC External Tables, Doris can access external tables via JDBC, the standard database access inferface. This allows Doris to visit various databases without tedious data ingestion, and give full play to its own OLAP capabilities to perform data analysis on external tables:

1. Multiple data sources can be connected to Doris;
2. It enables Join queries across Doris and other data sources and thus allows more complex analysis.

This topic introduces how to use JDBC External Tables in Doris.

</version>

### Create JDBC External Table in Doris

See [CREATE TABLE](https://doris.apache.org/docs/dev/sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE/) for syntax details.

#### 1. Create JDBC External Table by Creating JDBC_Resource

```sql
CREATE EXTERNAL RESOURCE jdbc_resource
properties (
    "type"="jdbc",
    "user"="root",
    "password"="123456",
    "jdbc_url"="jdbc:mysql://192.168.0.1:3306/test?useCursorFetch=true",
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

Parameter Description:

| Parameter        | Description                                                  |
| ---------------- | ------------------------------------------------------------ |
| **type**         | "jdbc"; required; specifies the type of the Resource         |
| **user**         | Username for accessing the external database                 |
| **password**     | Password of the user                                         |
| **jdbc_url**     | JDBC URL protocol, including the database type, IP address, port number, and database name; Please be aware of the different formats of different database protocols. For example, MySQL: "jdbc:mysql://127.0.0.1:3306/test?useCursorFetch=true". |
| **driver_class** | Class of the driver used to access the external database. For example, to access MySQL data: com.mysql.jdbc.Driver. |
| **driver_url**   | Driver URL for downloading the Jar file package that is used to access the external database, for example, http://IP:port/mysql-connector-java-5.1.47.jar. For local stand-alone testing, you can put the Jar file package in a local path: "driver_url"="file:///home/disk1/pathTo/mysql-connector-java-5.1.47.jar"; for local multi-machine testing, please ensure the consistency of the paths. |
| **resource**     | Name of the Resource that the Doris External Table depends on; should be the same as the name set in Resource creation. |
| **table**        | Name of the external table to be mapped in Doris             |
| **table_type**   | The database from which the external table comes, such as mysql, postgresql, sqlserver, and oracle. |

> **Note:**
>
> For local testing, please make sure you put the Jar file package in the FE and BE nodes, too.

<version since="1.2.1">

> In Doris 1.2.1 and newer versions, if you have put the driver in the  `jdbc_drivers`  directory of FE/BE, you can simply specify the file name in the driver URL: `"driver_url" = "mysql-connector-java-5.1.47.jar"`, and the system will automatically find the file in the `jdbc_drivers` directory.

</version>

### Query

```
select * from mysql_table where k1 > 1000 and k3 ='term';
```

In some cases, the keywords in the database might be used as the field names. For queries to function normally in these cases, Doris will add escape characters to the field names and tables names in SQL statements based on the rules of different databases, such as (``) for MySQL, ([]) for SQLServer, and ("") for PostgreSQL and Oracle. This might require extra attention on case sensitivity. You can view the query statements sent to these various databases via ```explain sql```.

### Write Data

After creating a JDBC External Table in Doris, you can write data or query results to it using the `insert into` statement. You can also ingest data from one JDBC External Table to another JDBC External Table.


```
insert into mysql_table values(1, "doris");
insert into mysql_table select * from table;
```

#### Transaction

In Doris, data is written to External Tables in batches. If the ingestion process is interrupted, rollbacks might be required. That's why JDBC External Tables support data writing transactions. You can utilize this feature by setting the session variable: `enable_odbc_transcation ` (ODBC transactions are also controlled by this variable).

```
set enable_odbc_transcation = true; 
```

The transaction mechanism ensures the atomicity of data writing to JDBC External Tables, but it reduces performance to a certain extent. You may decide whether to enable transactions based on your own tradeoff.

#### 1.MySQL Test

| MySQL Version | MySQL JDBC Driver Version       |
| ------------- | ------------------------------- |
| 8.0.30        | mysql-connector-java-5.1.47.jar |

#### 2.PostgreSQL Test

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

#### 3.SQLServer Test

| SQLServer Version | SQLServer JDBC Driver Version |
| ----------------- | ----------------------------- |
| 2022              | mssql-jdbc-11.2.0.jre8.jar    |

#### 4.Oracle Test

| Oracle Version | Oracle JDBC Driver Version |
| -------------- | -------------------------- |
| 11             | ojdbc6.jar                 |

Test information on more versions will be provided in the future.

#### 5.ClickHouse Test

| ClickHouse Version | ClickHouse JDBC Driver Version        |
| ------------------ | ------------------------------------- |
| 22           | clickhouse-jdbc-0.3.2-patch11-all.jar |
| 22           | clickhouse-jdbc-0.4.1-all.jar         |

#### 6.Sap Hana Test

| Sap Hana Version | Sap Hana JDBC Driver Version |
|------------------|------------------------------|
| 2.0              | ngdbc.jar                    |

```sql
CREATE EXTERNAL RESOURCE jdbc_hana
properties (
    "type"="jdbc",
    "user"="SYSTEM",
    "password"="SAPHANA",
    "jdbc_url" = "jdbc:sap://localhost:31515/TEST",
    "driver_url" = "file:///path/to/ngdbc.jar",
    "driver_class" = "com.sap.db.jdbc.Driver"
);

CREATE EXTERNAL TABLE `ext_hana` (
  `k1` int
) ENGINE=JDBC
PROPERTIES (
    "resource" = "jdbc_hana",
    "table" = "TEST.HANA",
    "table_type"="sap_hana"
);
```

#### 7.Trino Test

| Trino Version | Trino JDBC Driver Version |
|---------------|---------------------------|
| 389           | trino-jdbc-389.jar        |

```sql
CREATE EXTERNAL RESOURCE jdbc_trino
properties (
    "type"="jdbc",
    "user"="hadoop",
    "password"="",
    "jdbc_url" = "jdbc:trino://localhost:8080/hive",
    "driver_url" = "file:///path/to/trino-jdbc-389.jar",
    "driver_class" = "io.trino.jdbc.TrinoDriver"
);

CREATE EXTERNAL TABLE `ext_trino` (
  `k1` int
) ENGINE=JDBC
PROPERTIES (
    "resource" = "jdbc_trino",
    "table" = "hive.test",
    "table_type"="trino"
);
```

#### 8.OceanBase Test

| OceanBase Version | OceanBase JDBC Driver Version |
|-------------------|-------------------------------|
| 3.2.3             | oceanbase-client-2.4.2.jar    |

```sql
CREATE EXTERNAL RESOURCE jdbc_oceanbase
properties (
    "type"="jdbc",
    "user"="root",
    "password"="",
    "jdbc_url" = "jdbc:oceanbase://localhost:2881/test",
    "driver_url" = "file:///path/to/oceanbase-client-2.4.2.jar",
    "driver_class" = "com.oceanbase.jdbc.Driver",
    "oceanbase_mode" = "mysql" or "oracle"
);

CREATE EXTERNAL TABLE `ext_oceanbase` (
  `k1` int
) ENGINE=JDBC
PROPERTIES (
    "resource" = "jdbc_oceanbase",
    "table" = "test.test",
    "table_type"="oceanbase"
);
```

> **Note:**
>
> When creating an OceanBase external table, you only need to specify the `oceanbase mode` parameter when creating a resource, and the table type of the table to be created is oceanbase

## Type Mapping

The followings list how data types in different databases are mapped in Doris.

### MySQL

|      MySQL      |  Doris   |
| :-------------: | :------: |
|     BOOLEAN     | BOOLEAN  |
|     BIT(1)      | BOOLEAN  |
|     TINYINT     | TINYINT  |
|    SMALLINT     | SMALLINT |
|       INT       |   INT    |
|     BIGINT      |  BIGINT  |
| BIGINT UNSIGNED | LARGEINT |
|     VARCHAR     | VARCHAR  |
|      DATE       |   DATE   |
|      FLOAT      |  FLOAT   |
|    DATETIME     | DATETIME |
|     DOUBLE      |  DOUBLE  |
|     DECIMAL     | DECIMAL  |


### PostgreSQL

| PostgreSQL |  Doris   |
| :--------: | :------: |
|  BOOLEAN   | BOOLEAN  |
|  SMALLINT  | SMALLINT |
|    INT     |   INT    |
|   BIGINT   |  BIGINT  |
|  VARCHAR   | VARCHAR  |
|    DATE    |   DATE   |
| TIMESTAMP  | DATETIME |
|    REAL    |  FLOAT   |
|   FLOAT    |  DOUBLE  |
|  DECIMAL   | DECIMAL  |

### Oracle

|  Oracle  |  Doris   |
| :------: | :------: |
| VARCHAR  | VARCHAR  |
|   DATE   | DATETIME |
| SMALLINT | SMALLINT |
|   INT    |   INT    |
|   REAL   |  DOUBLE  |
|  FLOAT   |  DOUBLE  |
|  NUMBER  | DECIMAL  |


### SQL server

| SQLServer |  Doris   |
| :-------: | :------: |
|    BIT    | BOOLEAN  |
|  TINYINT  | TINYINT  |
| SMALLINT  | SMALLINT |
|    INT    |   INT    |
|  BIGINT   |  BIGINT  |
|  VARCHAR  | VARCHAR  |
|   DATE    |   DATE   |
| DATETIME  | DATETIME |
|   REAL    |  FLOAT   |
|   FLOAT   |  DOUBLE  |
|  DECIMAL  | DECIMAL  |

### ClickHouse

|                       ClickHouse                        |          Doris           |
|:-------------------------------------------------------:|:------------------------:|
|                         Boolean                         |         BOOLEAN          |
|                         String                          |          STRING          |
|                       Date/Date32                       |       DATE/DATEV2        |
|                   DateTime/DateTime64                   |   DATETIME/DATETIMEV2    |
|                         Float32                         |          FLOAT           |
|                         Float64                         |          DOUBLE          |
|                          Int8                           |         TINYINT          |
|                       Int16/UInt8                       |         SMALLINT         |
|                      Int32/UInt16                       |           INT            |
|                      Int64/Uint32                       |          BIGINT          |
|                      Int128/UInt64                      |         LARGEINT         |
|                 Int256/UInt128/UInt256                  |          STRING          |
|                         Decimal                         | DECIMAL/DECIMALV3/STRING |
|                   Enum/IPv4/IPv6/UUID                   |          STRING          |
| <version since="dev" type="inline"> Array(T) </version> |        ARRAY\<T\>        |


**Note:**

- <version since="dev" type="inline"> For Array types in ClickHouse, use Doris's Array type to match them. For basic types in an Array, see Basic type matching rules. Nested arrays are not supported. </version>
- Some data types in ClickHouse, such as UUID, IPv4, IPv6, and Enum8, will be mapped to Varchar/String in Doris. IPv4 and IPv6 will be displayed with an `/` as a prefix. You can use the `split_part` function to remove the `/` .
- The Point Geo type in ClickHouse cannot be mapped in Doris by far. 

### SAP HANA

|   SAP_HANA   |        Doris        |
|:------------:|:-------------------:|
|   BOOLEAN    |       BOOLEAN       |
|   TINYINT    |       TINYINT       |
|   SMALLINT   |      SMALLINT       |
|   INTERGER   |         INT         |
|    BIGINT    |       BIGINT        |
| SMALLDECIMAL |  DECIMAL/DECIMALV3  |
|   DECIMAL    |  DECIMAL/DECIMALV3  |
|     REAL     |        FLOAT        |
|    DOUBLE    |       DOUBLE        |
|     DATE     |     DATE/DATEV2     |
|     TIME     |        TEXT         |
|  TIMESTAMP   | DATETIME/DATETIMEV2 |
|  SECONDDATE  | DATETIME/DATETIMEV2 |
|   VARCHAR    |        TEXT         |
|   NVARCHAR   |        TEXT         |
|   ALPHANUM   |        TEXT         |
|  SHORTTEXT   |        TEXT         |
|     CHAR     |        CHAR         |
|    NCHAR     |        CHAR         |

### Trino

|   Trino   |        Doris        |
|:---------:|:-------------------:|
|  boolean  |       BOOLEAN       |
|  tinyint  |       TINYINT       |
| smallint  |      SMALLINT       |
|  integer  |         INT         |
|  bigint   |       BIGINT        |
|  decimal  |  DECIMAL/DECIMALV3  |
|   real    |        FLOAT        |
|  double   |       DOUBLE        |
|   date    |     DATE/DATEV2     |
| timestamp | DATETIME/DATETIMEV2 |
|  varchar  |        TEXT         |
|   char    |        CHAR         |
|   array   |        ARRAY        |
|  others   |     UNSUPPORTED     |

### OceanBase

For MySQL mode, please refer to [MySQL type mapping](#MySQL)
For Oracle mode, please refer to [Oracle type mapping](#Oracle)

## Q&A

See the FAQ section in [JDBC](https://doris.apache.org/docs/dev/lakehouse/multi-catalog/jdbc/).

