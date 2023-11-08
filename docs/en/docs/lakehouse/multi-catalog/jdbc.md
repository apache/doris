---
{
    "title": "JDBC",
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


# JDBC

JDBC Catalogs in Doris are connected to external data sources using the standard JDBC protocol.

Once connected, Doris will ingest metadata of databases and tables from the external data sources in order to enable quick access to external data.

## Usage

Supported datas sources include MySQL, PostgreSQL, Oracle, SQLServer, Clickhouse, Doris, SAP HANA, Trino and OceanBase.

## Syntax

```sql
CREATE CATALOG <catalog_name>
PROPERTIES ("key"="value", ...)
```

## Parameter Description

| Parameter                 | Required or Not | Default Value | Description                                                                                                              |
|---------------------------|-----------------|---------------|--------------------------------------------------------------------------------------------------------------------------|
| `user`                    | Yes             |               | Username in relation to the corresponding database                                                                       |
| `password`                | Yes             |               | Password for the corresponding database                                                                                  |
| `jdbc_url `               | Yes             |               | JDBC connection string                                                                                                   |
| `driver_url `             | Yes             |               | JDBC Driver Jar                                                                                                          |
| `driver_class `           | Yes             |               | JDBC Driver Class                                                                                                        |
| `only_specified_database` | No              | "false"       | Whether only the database specified to be synchronized.                                                                  |
| `lower_case_table_names`  | No              | "false"       | Whether to synchronize jdbc external data source table names in lower case.                                              |
| `include_database_list`   | No              | ""            | When only_specified_database=true，only synchronize the specified databases. split with ','. db name is case sensitive.   |
| `exclude_database_list`   | No              | ""            | When only_specified_database=true，do not synchronize the specified databases. split with ','. db name is case sensitive. |

### Driver path

`driver_url` can be specified in three ways:

1. File name. For example,  `mysql-connector-java-5.1.47.jar`. Please place the Jar file package in  `jdbc_drivers/`  under the FE/BE deployment directory in advance so the system can locate the file. You can change the location of the file by modifying  `jdbc_drivers_dir`  in fe.conf and be.conf.

2. Local absolute path. For example, `file:///path/to/mysql-connector-java-5.1.47.jar`. Please place the Jar file package in the specified paths of FE/BE node.

3. HTTP address. For example, `https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com/jdbc_driver/mysql-connector-java-8.0.25.jar`. The system will download the Driver file from the HTTP address. This only supports HTTP services with no authentication requirements.

### Lowercase table name synchronization

When `lower_case_table_names` is set to `true`, Doris is able to query non-lowercase databases and tables by maintaining a mapping of lowercase names to actual names on the remote system

**Notice:**

1. In versions before Doris 2.0.3, it is only valid for Oracle database. When querying, all library names and table names will be converted to uppercase before querying Oracle, for example:

   Oracle has the TEST table in the TEST space. When Doris creates the Catalog, set `lower_case_table_names` to `true`, then Doris can query the TEST table through `select * from oracle_catalog.test.test`, and Doris will automatically format test.test into TEST.TEST is sent to Oracle. It should be noted that this is the default behavior, which also means that lowercase table names in Oracle cannot be queried.

   For other databases, you still need to specify the real library name and table name when querying.

2. In Doris 2.0.3 and later versions, it is valid for all databases. When querying, all library names and table names will be converted into real names and then queried. If you upgrade from an old version to 2.0. 3, `Refresh <catalog_name>` is required to take effect.

   However, if the database or table names differ only in case, such as `Doris` and `doris`, Doris cannot query them due to ambiguity.

3. When the FE parameter's `lower_case_table_names` is set to `1` or `2`, the JDBC Catalog's `lower_case_table_names` parameter must be set to `true`. If the FE parameter's `lower_case_table_names` is set to `0`, the JDBC Catalog parameter can be `true` or `false` and defaults to `false`. This ensures consistency and predictability in how Doris handles internal and external table configurations.

### Specify synchronization database:

`only_specified_database`:
When the JDBC is connected, you can specify which database/schema to connect. For example, you can specify the DataBase in mysql `jdbc_url`; you can specify the CurrentSchema in PG `jdbc_url`.

`include_database_list`:
It only takes effect when `only_specified_database=true`, specify the database that needs to be synchronized, separated by ',', and the db name is case-sensitive.

`exclude_database_list`:
It only takes effect when `only specified database=true`, specifies multiple databases that do not need to be synchronized, separated by ',', and the db name is case-sensitive.

When `include_database_list` and `exclude_database_list` specify overlapping databases, `exclude_database_list` would take effect with higher privilege over `include_database_list`.

If you connect the Oracle database when using this property, please  use the version of the jar package above 8 or more (such as ojdbc8.jar).

## Query

### Example

```sql
select * from mysql_catalog.mysql_database.mysql_table where k1 > 1000 and k3 ='term';
```
:::tip
In some cases, the keywords in the database might be used as the field names. For queries to function normally in these cases, Doris will add escape characters to the field names and tables names in SQL statements based on the rules of different databases, such as (``) for MySQL, ([]) for SQLServer, and ("") for PostgreSQL and Oracle. This might require extra attention on case sensitivity. You can view the query statements sent to these various databases via ```explain sql```.
:::

### Predicate Pushdown

1. When executing a query like `where dt = '2022-01-01'`, Doris can push down these filtering conditions to the external data source, thereby directly excluding data that does not meet the conditions at the data source level, reducing the number of unqualified Necessary data acquisition and transfer. This greatly improves query performance while also reducing the load on external data sources.
   
2. When `enable_func_pushdown` is set to true, the function condition after where will also be pushed down to the external data source. Currently, only MySQL is supported. If you encounter a function that MySQL does not support, you can set this parameter to false, at present, Doris will automatically identify some functions not supported by MySQL to filter the push-down conditions, which can be checked by explain sql.

Functions that are currently not pushed down include:

|    MYSQL     |
|:------------:|
|  DATE_TRUNC  |
| MONEY_FORMAT |

### Line Limit

If there is a limit keyword in the query, Doris will translate it into semantics suitable for different data sources.

## Write Data

After the JDBC Catalog is established in Doris, you can write data directly through the insert into statement, or write the results of the query executed by Doris into the JDBC Catalog, or import data from one JDBC Catalog to another JDBC Catalog.

### Example

```sql
insert into mysql_catalog.mysql_database.mysql_table values(1, "doris");
insert into mysql_catalog.mysql_database.mysql_table select * from table;
```

### Transaction

In Doris, data is written to External Tables in batches. If the ingestion process is interrupted, rollbacks might be required. That's why JDBC Catalog Tables support data writing transactions. You can utilize this feature by setting the session variable: `enable_odbc_transcation `.

```
set enable_odbc_transcation = true; 
```

The transaction mechanism ensures the atomicity of data writing to JDBC External Tables, but it reduces performance to a certain extent. You may decide whether to enable transactions based on your own tradeoff.

## Guide

### MySQL

#### Example

* mysql 5.7

```sql
CREATE CATALOG jdbc_mysql PROPERTIES (
    "type"="jdbc",
    "user"="root",
    "password"="123456",
    "jdbc_url" = "jdbc:mysql://127.0.0.1:3306/demo",
    "driver_url" = "mysql-connector-java-5.1.47.jar",
    "driver_class" = "com.mysql.jdbc.Driver"
)
```

* mysql 8

```sql
CREATE CATALOG jdbc_mysql PROPERTIES (
    "type"="jdbc",
    "user"="root",
    "password"="123456",
    "jdbc_url" = "jdbc:mysql://127.0.0.1:3306/demo",
    "driver_url" = "mysql-connector-java-8.0.25.jar",
    "driver_class" = "com.mysql.cj.jdbc.Driver"
)
```

#### Hierarchy Mapping

|  Doris   |    MySQL     |
|:--------:|:------------:|
| Catalog  | MySQL Server |
| Database |   Database   |
|  Table   |    Table     |

#### Type Mapping

| MYSQL Type                                | Doris Type     | Comment                                                                       |
|-------------------------------------------|----------------|-------------------------------------------------------------------------------|
| BOOLEAN                                   | TINYINT        |                                                                               |
| TINYINT                                   | TINYINT        |                                                                               |
| SMALLINT                                  | SMALLINT       |                                                                               |
| MEDIUMINT                                 | INT            |                                                                               |
| INT                                       | INT            |                                                                               |
| BIGINT                                    | BIGINT         |                                                                               |
| UNSIGNED TINYINT                          | SMALLINT       | Doris does not have an UNSIGNED data type, so expand by an order of magnitude |
| UNSIGNED MEDIUMINT                        | INT            | Doris does not have an UNSIGNED data type, so expand by an order of magnitude |
| UNSIGNED INT                              | BIGINT         | Doris does not have an UNSIGNED data type, so expand by an order of magnitude |
| UNSIGNED BIGINT                           | LARGEINT       |                                                                               |
| FLOAT                                     | FLOAT          |                                                                               |
| DOUBLE                                    | DOUBLE         |                                                                               |
| DECIMAL                                   | DECIMAL        |                                                                               |
| DATE                                      | DATE           |                                                                               |
| TIMESTAMP                                 | DATETIME       |                                                                               |
| DATETIME                                  | DATETIME       |                                                                               |
| YEAR                                      | SMALLINT       |                                                                               |
| TIME                                      | STRING         |                                                                               |
| CHAR                                      | CHAR           |                                                                               |
| VARCHAR                                   | VARCHAR        |                                                                               |
| JSON                                      | JSON           |                                                                               |
| SET                                       | STRING         |                                                                               |
| BIT                                       | BOOLEAN/STRING | BIT(1) will be mapped to BOOLEAN, and other BITs will be mapped to STRING     |
| TINYTEXT、TEXT、MEDIUMTEXT、LONGTEXT         | STRING         |                                                                               |
| BLOB、MEDIUMBLOB、LONGBLOB、TINYBLOB         | STRING         |                                                                               |
| TINYSTRING、STRING、MEDIUMSTRING、LONGSTRING | STRING         |                                                                               |
| BINARY、VARBINARY                          | STRING         |                                                                               |
| Other                                     | UNSUPPORTED    |                                                                               |

### PostgreSQL

#### Example

```sql
CREATE CATALOG jdbc_postgresql PROPERTIES (
    "type"="jdbc",
    "user"="root",
    "password"="123456",
    "jdbc_url" = "jdbc:postgresql://127.0.0.1:5432/demo",
    "driver_url" = "postgresql-42.5.1.jar",
    "driver_class" = "org.postgresql.Driver"
);
```

#### Hierarchy Mapping

As for data mapping from PostgreSQL to Doris, one Database in Doris corresponds to one schema in the specified database in PostgreSQL (for example, "demo" in `jdbc_url`  above), and one Table in that Database corresponds to one table in that schema. To make it more intuitive, the mapping relations are as follows:

|  Doris   | PostgreSQL |
|:--------:|:----------:|
| Catalog  |  Database  |
| Database |   Schema   |
|  Table   |   Table    |

:::tip
Doris obtains all schemas that PG user can access through the SQL statement: `select nspname from pg_namespace where has_schema_privilege('<UserName>', nspname, 'USAGE');` and map these schemas to doris database.   
:::

#### Type Mapping

 | POSTGRESQL Type                         | Doris Type     | Comment                                   |
 |-----------------------------------------|----------------|-------------------------------------------|
 | boolean                                 | BOOLEAN        |                                           |
 | smallint/int2                           | SMALLINT       |                                           |
 | integer/int4                            | INT            |                                           |
 | bigint/int8                             | BIGINT         |                                           |
 | decimal/numeric                         | DECIMAL        |                                           |
 | real/float4                             | FLOAT          |                                           |
 | double precision                        | DOUBLE         |                                           |
 | smallserial                             | SMALLINT       |                                           |
 | serial                                  | INT            |                                           |
 | bigserial                               | BIGINT         |                                           |
 | char                                    | CHAR           |                                           |
 | varchar/text                            | STRING         |                                           |
 | timestamp                               | DATETIME       |                                           |
 | date                                    | DATE           |                                           |
 | json/josnb                              | JSON           |                                           |
 | time                                    | STRING         |                                           |
 | interval                                | STRING         |                                           |
 | point/line/lseg/box/path/polygon/circle | STRING         |                                           |
 | cidr/inet/macaddr                       | STRING         |                                           |
 | bit                                     | BOOLEAN/STRING | bit(1) will be mapped to BOOLEAN, and other bits will be mapped to STRING |
 | uuid                                    | STRING         |                                           |
 | Other                                   | UNSUPPORTED    |                                           |

### Oracle

#### Example

```sql
CREATE CATALOG jdbc_oracle PROPERTIES (
    "type"="jdbc",
    "user"="root",
    "password"="123456",
    "jdbc_url" = "jdbc:oracle:thin:@127.0.0.1:1521:helowin",
    "driver_url" = "ojdbc8.jar",
    "driver_class" = "oracle.jdbc.driver.OracleDriver"
);
```

#### Hierarchy Mapping

As for data mapping from Oracle to Doris, one Database in Doris corresponds to one User, and one Table in that Database corresponds to one table that the User has access to. In conclusion, the mapping relations are as follows:

|  Doris   |  Oracle  |
|:--------:|:--------:|
| Catalog  | Database |
| Database |   User   |
|  Table   |  Table   |

**NOTE:** Synchronizing Oracle's SYNONYM TABLE is not currently supported.

#### Type Mapping

| ORACLE Type                       | Doris Type                           | Comment                                                                                                                                                                       |
|-----------------------------------|--------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| number(p) / number(p,0)           | TINYINT/SMALLINT/INT/BIGINT/LARGEINT | Doris will determine the type to map to based on the value of p: `p < 3` -> `TINYINT`; `p < 5` -> `SMALLINT`; `p < 10` -> `INT`; `p < 19` -> `BIGINT`; `p > 19` -> `LARGEINT` |
| number(p,s), [ if(s>0 && p>s) ]   | DECIMAL(p,s)                         |                                                                                                                                                                               |
| number(p,s), [ if(s>0 && p < s) ] | DECIMAL(s,s)                         |                                                                                                                                                                               |
| number(p,s), [ if(s<0) ]          | TINYINT/SMALLINT/INT/BIGINT/LARGEINT | if s<0, Doris will set `p` to `p+|s|`, and perform the same mapping as `number(p) / number(p,0)`.                                                                             |
| number                            |                                      | Doris does not support Oracle `NUMBER` type that does not specified p and s                                                                                                   |
| float/real                        | DOUBLE                               |                                                                                                                                                                               |
| DATE                              | DATETIME                             |                                                                                                                                                                               |
| TIMESTAMP                         | DATETIME                             |                                                                                                                                                                               |
| CHAR/NCHAR                        | STRING                               |                                                                                                                                                                               |
| VARCHAR2/NVARCHAR2                | STRING                               |                                                                                                                                                                               |
| LONG/ RAW/ LONG RAW/ INTERVAL     | STRING                               |                                                                                                                                                                               |
| Other                             | UNSUPPORTED                          |                                                                                                                                                                               |

### SQLServer

#### Example

```sql
CREATE CATALOG jdbc_sqlserve PROPERTIES (
    "type"="jdbc",
    "user"="SA",
    "password"="Doris123456",
    "jdbc_url" = "jdbc:sqlserver://localhost:1433;DataBaseName=doris_test",
    "driver_url" = "mssql-jdbc-11.2.3.jre8.jar",
    "driver_class" = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
);
```

#### Hierarchy Mapping

As for data mapping from SQLServer to Doris, one Database in Doris corresponds to one schema in the specified database in SQLServer (for example, "doris_test" in `jdbc_url`  above), and one Table in that Database corresponds to one table in that schema. The mapping relations are as follows:

|  Doris   | SQLServer |
|:--------:|:---------:|
| Catalog  | Database  |
| Database |  Schema   |
|  Table   |   Table   |

#### Type Mapping

| SQLServer Type                         | Doris Type    | Comment                                                                  |
|----------------------------------------|---------------|--------------------------------------------------------------------------|
| bit                                    | BOOLEAN       |                                                                          |
| tinyint                                | SMALLINT      | SQLServer's tinyint is an unsigned number, so it maps to Doris' SMALLINT |
| smallint                               | SMALLINT      |                                                                          |
| int                                    | INT           |                                                                          |
| bigint                                 | BIGINT        |                                                                          |
| real                                   | FLOAT         |                                                                          |
| float                                  | DOUBLE        |                                                                          |
| money                                  | DECIMAL(19,4) |                                                                          |
| smallmoney                             | DECIMAL(10,4) |                                                                          |
| decimal/numeric                        | DECIMAL       |                                                                          |
| date                                   | DATE          |                                                                          |
| datetime/datetime2/smalldatetime       | DATETIMEV2    |                                                                          |
| char/varchar/text/nchar/nvarchar/ntext | STRING        |                                                                          |
| binary/varbinary                       | STRING        |                                                                          |
| time/datetimeoffset                    | STRING        |                                                                          |
| Other                                  | UNSUPPORTED   |                                                                          |

### Doris

Jdbc Catalog also support to connect another Doris database:

* mysql 5.7 Driver

```sql
CREATE CATALOG jdbc_doris PROPERTIES (
    "type"="jdbc",
    "user"="root",
    "password"="123456",
    "jdbc_url" = "jdbc:mysql://127.0.0.1:9030?useSSL=false",
    "driver_url" = "mysql-connector-java-5.1.47.jar",
    "driver_class" = "com.mysql.jdbc.Driver"
)
```

* mysql 8 Driver

```sql
CREATE CATALOG jdbc_doris PROPERTIES (
    "type"="jdbc",
    "user"="root",
    "password"="123456",
    "jdbc_url" = "jdbc:mysql://127.0.0.1:9030?useSSL=false",
    "driver_url" = "mysql-connector-java-8.0.25.jar",
    "driver_class" = "com.mysql.cj.jdbc.Driver"
)
```

#### Type Mapping

| Doris Type | Jdbc Catlog Doris Type | Comment                                                                              |
|------------|------------------------|--------------------------------------------------------------------------------------|
| BOOLEAN    | BOOLEAN                |                                                                                      |
| TINYINT    | TINYINT                |                                                                                      |
| SMALLINT   | SMALLINT               |                                                                                      |
| INT        | INT                    |                                                                                      |
| BIGINT     | BIGINT                 |                                                                                      |
| LARGEINT   | LARGEINT               |                                                                                      |
| FLOAT      | FLOAT                  |                                                                                      |
| DOUBLE     | DOUBLE                 |                                                                                      |
| DECIMALV3  | DECIMALV3/STRING       | Which type will be selected according to the (precision, scale) of the DECIMAL field |
| DATE       | DATE                   |                                                                                      |
| DATETIME   | DATETIME               |                                                                                      |
| CHAR       | CHAR                   |                                                                                      |
| VARCHAR    | VARCHAR                |                                                                                      |
| STRING     | STRING                 |                                                                                      |
| TEXT       | STRING                 |                                                                                      |
| HLL        | HLL                    | Query HLL needs to set `return_object_data_as_binary=true`                           |
| Array      | Array                  | The internal type adaptation logic of Array refers to the above types, and nested complex types are not supported        |
| BITMAP     | BITMAP                 | Query BITMAP needs to set `return_object_data_as_binary=true`                        |
| Other      | UNSUPPORTED            |                                                                                      |

### Clickhouse

#### Example

```sql
CREATE CATALOG jdbc_clickhouse PROPERTIES (
    "type"="jdbc",
    "user"="root",
    "password"="123456",
    "jdbc_url" = "jdbc:clickhouse://127.0.0.1:8123/demo",
    "driver_url" = "clickhouse-jdbc-0.4.2-all.jar",
    "driver_class" = "com.clickhouse.jdbc.ClickHouseDriver"
);
```

#### Hierarchy Mapping

|  Doris   |    ClickHouse     |
|:--------:|:-----------------:|
| Catalog  | ClickHouse Server |
| Database |     Database      |
|  Table   |       Table       |

#### Type Mapping

| ClickHouse Type        | Doris Type       | Comment                                                                                                  |
|------------------------|------------------|----------------------------------------------------------------------------------------------------------|
| Bool                   | BOOLEAN          |                                                                                                          |
| String                 | STRING           |                                                                                                          |
| Date/Date32            | DATE             |                                                                                                          |
| DateTime/DateTime64    | DATETIME         |                                                                                                          |
| Float32                | FLOAT            |                                                                                                          |
| Float64                | DOUBLE           |                                                                                                          |
| Int8                   | TINYINT          |                                                                                                          |
| Int16/UInt8            | SMALLINT         | Doris does not have an UNSIGNED data type, so expand by an order of magnitude                            |
| Int32/UInt16           | INT              | Doris does not have an UNSIGNED data type, so expand by an order of magnitude                            |
| Int64/Uint32           | BIGINT           | Doris does not have an UNSIGNED data type, so expand by an order of magnitude                            |
| Int128/UInt64          | LARGEINT         | Doris does not have an UNSIGNED data type, so expand by an order of magnitude                            |
| Int256/UInt128/UInt256 | STRING           | Doris does not have a data type of this magnitude, and uses STRING for processing                        |
| DECIMAL                | DECIMALV3/STRING | Which type will be selected according to the (precision, scale) of the DECIMAL field                     |
| Enum/IPv4/IPv6/UUID    | STRING           |                                                                                                          |
| Array                  | ARRAY            | The internal type adaptation logic of Array refers to the above types, and does not support nested types |
| Other                  | UNSUPPORTED      |                                                                                                          |


### SAP HANA

#### Example

```sql
CREATE CATALOG jdbc_hana PROPERTIES (
    "type"="jdbc",
    "user"="SYSTEM",
    "password"="SAPHANA",
    "jdbc_url" = "jdbc:sap://localhost:31515/TEST",
    "driver_url" = "ngdbc.jar",
    "driver_class" = "com.sap.db.jdbc.Driver"
)
```

#### Hierarchy Mapping

|  Doris   | SAP HANA |
|:--------:|:--------:|
| Catalog  | Database |
| Database |  Schema  |
|  Table   |  Table   |

#### Type Mapping

| SAP HANA Type | Doris Type       | Comment                                                                              |
|---------------|------------------|--------------------------------------------------------------------------------------|
| BOOLEAN       | BOOLEAN          |                                                                                      |
| TINYINT       | TINYINT          |                                                                                      |
| SMALLINT      | SMALLINT         |                                                                                      |
| INTERGER      | INT              |                                                                                      |
| BIGINT        | BIGINT           |                                                                                      |
| SMALLDECIMAL  | DECIMALV3        |                                                                                      |
| DECIMAL       | DECIMALV3/STRING | Which type will be selected according to the (precision, scale) of the DECIMAL field |
| REAL          | FLOAT            |                                                                                      |
| DOUBLE        | DOUBLE           |                                                                                      |
| DATE          | DATE             |                                                                                      |
| TIME          | STRING           |                                                                                      |
| TIMESTAMP     | DATETIME         |                                                                                      |
| SECONDDATE    | DATETIME         |                                                                                      |
| VARCHAR       | STRING           |                                                                                      |
| NVARCHAR      | STRING           |                                                                                      |
| ALPHANUM      | STRING           |                                                                                      |
| SHORTTEXT     | STRING           |                                                                                      |
| CHAR          | CHAR             |                                                                                      |
| NCHAR         | CHAR             |                                                                                      |


### Trino/Presto

#### Example

* Trino

```sql
CREATE CATALOG jdbc_trino PROPERTIES (
    "type"="jdbc",
    "user"="hadoop",
    "password"="",
    "jdbc_url" = "jdbc:trino://localhost:9000/hive",
    "driver_url" = "trino-jdbc-389.jar",
    "driver_class" = "io.trino.jdbc.TrinoDriver"
);
```

* Presto

```sql
CREATE CATALOG jdbc_presto PROPERTIES (
    "type"="jdbc",
    "user"="hadoop",
    "password"="",
    "jdbc_url" = "jdbc:presto://localhost:9000/hive",
    "driver_url" = "presto-jdbc-0.280.jar",
    "driver_class" = "com.facebook.presto.jdbc.PrestoDriver"
);
```

#### Hierarchy Mapping

When mapping Trino, Doris's Database corresponds to a Schema under the specified Catalog in Trino (such as "hive" in the `jdbc_url` parameter in the example). The Table under the Database of Doris corresponds to the Tables under the Schema in Trino. That is, the mapping relationship is as follows:

|  Doris   | Trino/Presto |
|:--------:|:------------:|
| Catalog  |   Catalog    |
| Database |    Schema    |
|  Table   |    Table     |


#### Type Mapping

| Trino/Presto Type | Doris Type               | Comment                                                                                                  |
|-------------------|--------------------------|----------------------------------------------------------------------------------------------------------|
| boolean           | BOOLEAN                  |                                                                                                          |
| tinyint           | TINYINT                  |                                                                                                          |
| smallint          | SMALLINT                 |                                                                                                          |
| integer           | INT                      |                                                                                                          |
| bigint            | BIGINT                   |                                                                                                          |
| decimal           | DECIMAL/DECIMALV3/STRING | Which type will be selected according to the (precision, scale) of the DECIMAL field                     |
| real              | FLOAT                    |                                                                                                          |
| double            | DOUBLE                   |                                                                                                          |
| date              | DATE                     |                                                                                                          |
| timestamp         | DATETIME                 |                                                                                                          |
| varchar           | TEXT                     |                                                                                                          |
| char              | CHAR                     |                                                                                                          |
| array             | ARRAY                    | The internal type adaptation logic of Array refers to the above types, and does not support nested types |
| others            | UNSUPPORTED              |                                                                                                          |


### OceanBase

#### Example

```sql
CREATE CATALOG jdbc_oceanbase PROPERTIES (
    "type"="jdbc",
    "user"="root",
    "password"="123456",
    "jdbc_url" = "jdbc:oceanbase://127.0.0.1:2881/demo",
    "driver_url" = "oceanbase-client-2.4.2.jar",
    "driver_class" = "com.oceanbase.jdbc.Driver"
)
```

:::tip
When Doris connects to OceanBase, it will automatically recognize that OceanBase is in MySQL or Oracle mode. Hierarchical correspondence and type mapping refer to [MySQL](#MySQL) and [Oracle](#Oracle)
:::

### View the JDBC Catalog

You can query all Catalogs in the current Doris cluster through SHOW CATALOGS:

```sql
SHOW CATALOGS;
```

Query the creation statement of a Catalog through SHOW CREATE CATALOG:

```sql
SHOW CREATE CATALOG <catalog_name>;
```

### Drop the JDBC Catalog

A Catalog can be deleted via DROP CATALOG:

```sql
DROP CATALOG <catalog_name>;
```

### Query the JDBC Catalog

1. Use SWITCH to switch the Catalog in effect for the current session:

    ```sql
    SWITCH <catalog_name>;
    ```

2. Query all libraries under the current Catalog through SHOW DATABASES:

    ```sql
    SHOW DATABASES FROM <catalog_name>;
    ```

    ```sql
    SHOW DATABASES;
    ```

3. Use USE to switch the Database that takes effect in the current session:

    ```sql
    USE <database_name>;
    ```

   Or directly use `USE <catalog_name>.<database_name>;` to switch the Database that takes effect in the current session

4. Query all tables under the current Catalog through SHOW TABLES:

    ```sql
    SHOW TABLES FROM <catalog_name>.<database_name>;
    ```

    ```sql
    SHOW TABLES FROM <database_name>;
    ```

    ```sql
    SHOW TABLES;
    ```

5. Query the data of a table under the current Catalog through SELECT:

    ```sql
    SELECT * FROM <table_name>;
    ```

## FAQ

1. Are there any other databases supported besides MySQL, Oracle, PostgreSQL, SQLServer, ClickHouse, SAP HANA, Trino and OceanBase?

   Currently, Doris supports MySQL, Oracle, PostgreSQL, SQLServer, ClickHouse, SAP HANA, Trino and OceanBase. We are planning to expand this list. Technically, any databases that support JDBC access can be connected to Doris in the form of JDBC external tables. You are more than welcome to be a Doris contributor to expedite this effort.

2. Why does Mojibake occur when Doris tries to read emojis from MySQL external tables?

   In MySQL, utf8mb3 is the default utf8 format. It cannot represent emojis, which require 4-byte encoding. To solve this, when creating MySQL external tables, you need to set utf8mb4 encoding for the corresponding columns, set the server encoding to utf8mb4, and leave the characterEncoding in JDBC URl empty (because utf8mb4 is not supported for this property, and anything other than utf8mb4 will cause a failure to write the emojis).

   You can modify the configuration items globally:

   ```
   Modify the my.ini file in the mysql directory (for linux system, modify the my.cnf file in the etc directory)
   [client]
   default-character-set=utf8mb4
   
   [mysql]
   Set the mysql default character set
   default-character-set=utf8mb4
   
   [mysqld]
   Set the mysql character set server
   character-set-server=utf8mb4
   collation-server=utf8mb4_unicode_ci
   init_connect='SET NAMES utf8mb4
   
   Modify the type for the corresponding tables and columns
   ALTER TABLE table_name MODIFY  colum_name  VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
   ALTER TABLE table_name CHARSET=utf8mb4;
   SET NAMES utf8mb4
   ```

3. Exception occurs when reading MySQL date/datetime type

    ```
    ERROR 1105 (HY000): errCode = 2, detailMessage = (10.16.10.6)[INTERNAL_ERROR]UdfRuntimeException: get next block failed:
    CAUSED BY: SQLException: Zero date value prohibited
    CAUSED BY: DataReadException: Zero date value prohibited
    ```

    This is because the default handling of illegal Date/DateTime in JDBC is to throw an exception, and this behavior can be controlled through the parameter `zeroDateTimeBehavior`.

    The optional parameters are: `EXCEPTION`, `CONVERT_TO_NULL`, `ROUND`, respectively: exception error reporting, converted to NULL value, converted to "0001-01-01 00:00:00";

    You need to add `zeroDateTimeBehavior=convertToNull` to the end of the JDBC connection string when creating the Catalog `jdbc_url`, such as `"jdbc_url" = "jdbc:mysql://127.0.0.1:3306/test?zeroDateTimeBehavior=convertToNull"`
    In this case, JDBC will convert 0000-00-00 or 0000-00-00 00:00:00 into null, and then Doris will process all Date/DateTime type columns in the current Catalog as nullable types, so that It can be read normally.

4. When reading the MySQL table or other tables, a class loading failure occurs.

    Such as the following exception:

    ```
    failed to load driver class com.mysql.jdbc.driver in either of hikariconfig class loader
    ```

    This is because when creating the catalog, the driver_class filled in is incorrect and needs to be filled in correctly. For example, the above example has a case problem and should be filled in as `"driver_class" = "com.mysql.jdbc.Driver"`

5. Communication link abnormality occurs when reading MySQL

    If the following error occurs:

    ```
    ERROR 1105 (HY000): errCode = 2, detailMessage = PoolInitializationException: Failed to initialize pool: Communications link failure
    
    The last packet successfully received from the server was 7 milliseconds ago. The last packet sent successfully to the server was 4 milliseconds ago.
    CAUSED BY: CommunicationsException: Communications link failure
        
    The last packet successfully received from the server was 7 milliseconds ago. The last packet sent successfully to the server was 4 milliseconds ago.
    CAUSED BY: SSLHandshakeExcepti
    ```

    You can view be’s be.out log

    If the following information is included:

    ```
    WARN: Establishing SSL connection without server's identity verification is not recommended.
    According to MySQL 5.5.45+, 5.6.26+ and 5.7.6+ requirements SSL connection must be established by default if explicit option isn't set.
    For compliance with existing applications not using SSL the verifyServerCertificate property is set to 'false'.
    You need either to explicitly disable SSL by setting useSSL=false, or set useSSL=true and provide truststore for server certificate verification.
    ```

   You can add `?useSSL=false` to the end of the JDBC connection string when creating the Catalog, such as `"jdbc_url" = "jdbc:mysql://127.0.0.1:3306/test?useSSL=false"`

6. When using JDBC to query large amounts of MYSQL data, if the query is occasionally successful, the following error will occasionally be reported. When this error occurs, all MYSQL connections are disconnected and cannot be connected to MYSQL SERVER. After a while, mysql returns to normal. , but the previous connections are gone:

    ```
    ERROR 1105 (HY000): errCode = 2, detailMessage = [INTERNAL_ERROR]UdfRuntimeException: JDBC executor sql has error:
    CAUSED BY: CommunicationsException: Communications link failure
    The last packet successfully received from the server was 4,446 milliseconds ago. The last packet sent successfully to the server was 4,446 milliseconds ago.
    ```

    When the above phenomenon occurs, it may be that Mysql Server's own memory or CPU resources are exhausted, causing the Mysql service to be unavailable. You can try to increase the memory or CPU configuration of Mysql Server.

7. During the process of using JDBC to query MYSQL, if it is found that the query results are inconsistent with the query results in the MYSQL library

    First, check whether the string in the query field is case-sensitive. For example, there is a field c_1 in Table that contains two pieces of data: "aaa" and "AAA". If no distinguishing string is specified when initializing the MYSQL database,
    Case, then MYSQL is not case-sensitive in strings by default, but in Doris it is strictly case-sensitive, so the following situations will occur:

    ```
    Mysql behavior:
    select count(c_1) from table where c_1 = "aaa"; The string size is not distinguished, so the result is: 2

    Doris behavior:
    select count(c_1) from table where c_1 = "aaa"; strictly distinguishes the string size, so the result is: 1
    ```

   If the above phenomenon occurs, it needs to be adjusted according to needs, as follows:

   Add the "BINARY" keyword to force case sensitivity when querying in MYSQL: select count(c_1) from table where BINARY c_1 = "aaa"; or specify when creating a table in MYSQL:
   CREATE TABLE table ( c_1 VARCHAR(255) CHARACTER SET binary ); Or specify collation rules to make case sensitive when initializing the MYSQL database:
   character-set-server=UTF-8 and collation-server=utf8_bin.

8. Communication link abnormality occurs when reading SQL Server

    ```
    ERROR 1105 (HY000): errCode = 2, detailMessage = (10.16.10.6)[CANCELLED][INTERNAL_ERROR]UdfRuntimeException: Initialize datasource failed:
    CAUSED BY: SQLServerException: The driver could not establish a secure connection to SQL Server by using Secure Sockets Layer (SSL) encryption.
    Error: "sun.security.validator.ValidatorException: PKIX path building failed: sun.security.provider.certpath.SunCertPathBuilderException:
    unable to find valid certification path to requested target". ClientConnectionId:a92f3817-e8e6-4311-bc21-7c66
    ```

   You can add `encrypt=false` to the end of the JDBC connection string when creating the Catalog, such as `"jdbc_url" = "jdbc:sqlserver://127.0.0.1:1433;DataBaseName=doris_test;encrypt=false"`

9. `Non supported character set (add orai18n.jar in your classpath): ZHS16GBK` exception occurs when reading Oracle

    Download [orai18n.jar](https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html) and put it in the lib directory of Doris FE and the lib/java_extensions directory of BE (versions before Doris 2.0 It needs to be placed in the lib directory of BE).

    Starting from version 2.0.2, this file can be placed in the `custom_lib/` directory of FE and BE (if it does not exist, just create it manually) to prevent the file from being lost when the lib directory is replaced when upgrading the cluster.

10. `NoClassDefFoundError: net/jpountz/lz4/LZ4Factory` error message appears when reading Clickhouse data through jdbc catalog

    You can download the [lz4-1.3.0.jar](https://repo1.maven.org/maven2/net/jpountz/lz4/lz4/1.3.0/lz4-1.3.0.jar) package first, and then put it in DorisFE lib directory and BE's `lib/lib/java_extensions` directory (versions before Doris 2.0 need to be placed in BE's lib directory).

    Starting from version 2.0.2, this file can be placed in the `custom_lib/` directory of FE and BE (if it does not exist, just create it manually) to prevent the file from being lost due to the replacement of the lib directory when upgrading the cluster.