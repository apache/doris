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

JDBC catalogs in Doris allows for connecting to external data sources using the standard JDBC protocol.

Once connected, Doris will ingest database and table metadata in order to enable quick access to data in the external data sources.

## Usage

Supported data sources are MySQL, PostgreSQL, Oracle, SQLServer, Clickhouse, Doris, SAP HANA, Trino and OceanBase.

## Syntax

```sql
CREATE CATALOG <catalog_name>
PROPERTIES ("key" = "value", ...)
```

## Parameter Description

| Parameter                 | Required or Not | Default Value | Description                                                                                                              |
|---------------------------|-----------------|---------------|--------------------------------------------------------------------------------------------------------------------------|
| `user`                    | Yes             |               | Username for the external database                                                                                       |
| `password`                | Yes             |               | Password for the external database                                                                                       |
| `jdbc_url `               | Yes             |               | JDBC connection string                                                                                                   |
| `driver_url `             | Yes             |               | JDBC driver JAR file                                                                                                     |
| `driver_class `           | Yes             |               | JDBC driver class                                                                                                        |
| `only_specified_database` | No              | false         | Whether only the specified database should be synchronized.                                                              |
| `lower_case_meta_names`   | No              | false         | Whether to synchronize the database name, table name and column name of the external JDBC data source in lowercase.      |
| `meta_names_mapping`      | No              |               | When the JDBC external data source has the same name but different case, e.g. DORIS and doris, Doris reports an error when querying the catalog due to ambiguity. In this case, the `meta_names_mapping` parameter needs to be specified to resolve the conflict. |
| `include_database_list`   | No              |               | When `only_specified_database = true`，only synchronize the specified databases. Separate with `,`. Database name is case sensitive. |
| `exclude_database_list`   | No              |               | When `only_specified_database = true`，do not synchronize the specified databases. Separate with `,`. Database name is case sensitive. |

### Driver path

`driver_url` can be specified in three ways:

1. File name. For example, `mysql-connector-java-8.0.25.jar`. Place the the JAR file in a directory `jdbc_drivers` under the Doris `fe` and `be` installation directories. I.e. create two directories `/fe/jdbc_drivers` and `/be/jdbc_drivers` and place a JAR file in each of them. You may have to create the `jdbc_drivers` directories manually. Make sure the directories are readable and preferably owned by the Doris user. You can change the location of the driver files by modifying the `jdbc_drivers_dir` property in the `fe.conf` and `be.conf` configuration files.

2. Local absolute path. For example, `file:///path/to/mysql-connector-java-8.0.25.jar`. Please place the JAR file in the specified paths of FE/BE node.

3. HTTP address. For example, `https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com/jdbc_driver/mysql-connector-java-8.0.25.jar`. The system will download the driver file from the HTTP address. This only supports HTTP and HTTPS services with no authentication requirements.

**Driver package security**

In order to prevent the use of a driver JAR package with a disallowed path when creating the catalog, Doris will perform path management and checksum checking on the JAR package.

1. For the above method 1, the `jdbc_drivers_dir` configured by the Doris default user and all JAR packages in its directory are safe and will not be path checked.

2. For the above methods 2 and 3, Doris will check the source of the JAR package. The checking rules are as follows:

   * Control the allowed driver package paths through the FE configuration item `jdbc_driver_secure_path`. This configuration item can configure multiple paths, separated by semicolons. When this item is configured, Doris will check whether the prefix of the `driver_url` path in the catalog properties is in `jdbc_driver_secure_path`. If not, it will refuse to create the catalog.
   * This parameter defaults to `*`, which means JAR packages of all paths are allowed.
   * If the configuration `jdbc_driver_secure_path` is empty, driver packages for all paths are not allowed, which means that the driver package can only be specified using method 1 above.

   > If you configure `jdbc_driver_secure_path = "file:///path/to/jdbc_drivers;http://path/to/jdbc_drivers"`, only `file:///path/to/jdbc_drivers` or `http:// is allowed The driver package path starting with path/to/jdbc_drivers`.

3. When creating a catalog, you can specify the checksum of the driver package through the `checksum` parameter. Doris will verify the driver package after loading the driver package. If the verification fails, it will refuse to create the catalog.

:::warning
The above verification will only be performed when the catalog is created. For already created catalogs, verification will not be performed again.
:::

### Lowercase name synchronization

When `lower_case_meta_names` is set to `true`, Doris maintains the mapping of lowercase names to actual names in the remote system, enabling queries to use lowercase to query non-lowercase databases, tables and columns of external data sources.

Since FE has the `lower_case_table_names` parameter, it will affect the table name case rules during query, so the rules are as follows

* When FE `lower_case_table_names` config is 0

  lower_case_meta_names = false, the case is consistent with the source library.
  lower_case_meta_names = true, lowercase repository table column names.

* When FE `lower_case_table_names` config is 1

  lower_case_meta_names = false, the case of db and column is consistent with the source library, but the table is stored in lowercase
  lower_case_meta_names = true, lowercase repository table column names.

* When FE `lower_case_table_names` config is 2

  lower_case_meta_names = false, the case is consistent with the source library.
  lower_case_meta_names = true, lowercase repository table column names.

If the parameter configuration when creating the catalog matches the lowercase conversion rule in the above rules, Doris will convert the corresponding name to lowercase and store it in Doris. When querying, you need to use the lowercase name displayed by Doris.

If the external data source has the same name but different case, such as DORIS and doris, Doris will report an error when querying the catalog due to ambiguity. In this case, you need to configure the `meta_names_mapping` parameter to resolve the conflict.

The `meta_names_mapping` parameter accepts a Json format string with the following format:

```json
{
  "databases": [
    {
      "remoteDatabase": "DORIS",
      "mapping": "doris_1"
    },
    {
      "remoteDatabase": "doris",
      "mapping": "doris_2"
    }],
  "tables": [
    {
      "remoteDatabase": "DORIS",
      "remoteTable": "DORIS",
      "mapping": "doris_1"
    },
    {
      "remoteDatabase": "DORIS",
      "remoteTable": "doris",
      "mapping": "doris_2"
    }],
  "columns": [
    {
      "remoteDatabase": "DORIS",
      "remoteTable": "DORIS",
      "remoteColumn": "DORIS",
      "mapping": "doris_1"
    },
    {
      "remoteDatabase": "DORIS",
      "remoteTable": "DORIS",
      "remoteColumn": "doris",
      "mapping": "doris_2"
    }]
}
```

When filling this configuration into the statement that creates the catalog, there are double quotes in Json, so you need to escape the double quotes or directly use single quotes to wrap the Json string when filling in.

```sql
CREATE CATALOG jdbc_catalog PROPERTIES (
    ...
    "meta_names_mapping" = "{\"databases\":[{\"remoteDatabase\":\"DORIS\",\"mapping\":\"doris_1\"},{\"remoteDatabase\":\"doris\",\"mapping\":\"doris_2\"}]}"
    ...
);
```

或者
```sql
CREATE CATALOG jdbc_catalog PROPERTIES (
    ...
    "meta_names_mapping" = '{"databases":[{"remoteDatabase":"DORIS","mapping":"doris_1"},{"remoteDatabase":"doris","mapping":"doris_2"}]}'
    ...
);

```

**Notice:**

JDBC catalog has the following three stages for mapping rules for external table case:

* Doris versions prior to 2.0.3

  This configuration name is `lower_case_table_names`, which is only valid for Oracle database. Setting this parameter to `true` in other data sources will affect the query, so please do not set it.

  When querying Oracle, all library names and table names will be converted to uppercase before querying Oracle, for example:

  Oracle has the TEST table in the TEST space. When Doris creates the catalog, set `lower_case_table_names` to `true`, then Doris can query the TEST table through `select * from oracle_catalog.test.test`, and Doris will automatically format test.test into TEST.TEST is sent to Oracle. It should be noted that this is the default behavior, which also means that lowercase table names in Oracle cannot be queried.

* Doris 2.0.3 version:

  This configuration is called `lower_case_table_names` and is valid for all databases. When querying, all library names and table names will be converted into real names and then queried. If you upgrade from an old version to 2.0.3, you need ` Refresh <catalog_name>` can take effect.

  However, if the library, table, or column names differ only in case, such as `Doris` and `doris`, Doris cannot query them due to ambiguity.

  And when the `lower_case_table_names` parameter of the FE parameter is set to `1` or `2`, the `lower_case_table_names` parameter of the JDBC catalog must be set to `true`. If the `lower_case_table_names` of the FE parameter is set to `0`, the JDBC catalog parameter can be `true` or `false`, defaulting to `false`.

* Doris 2.1.0 and later versions:

  In order to avoid confusion with the `lower_case_table_names` parameter of FE conf, this configuration name is changed to `lower_case_meta_names`, which is valid for all databases. During query, all library names, table names and column names will be converted into real names, and then Check it out. If you upgrade from an old version to 2.1.0, you need `Refresh <catalog_name>` to take effect.

  For specific rules, please refer to the introduction of `lower_case_meta_names` at the beginning of this section.

  Users who have previously set the JDBC catalog `lower_case_table_names` parameter will automatically have `lower_case_table_names` converted to `lower_case_meta_names` when upgrading to 2.0.4.

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

2. When variable `enable_ext_func_pred_pushdown` is set to true, the function conditions after where will also be pushed down to the external data source. Currently, only MySQL, ClickHouse, and Oracle are supported. If you encounter functions that are not supported by MySQL, ClickHouse, and Oracle, you can use this The parameter is set to false. At present, Doris will automatically identify some functions that are not supported by MySQL and functions supported by CLickHouse and Oracle for push-down condition filtering. You can view them through explain sql.

Functions that are currently not pushed down include:

|    MYSQL     |
|:------------:|
|  DATE_TRUNC  |
| MONEY_FORMAT |

Functions that are currently pushed down include:

|  ClickHouse    |
|:--------------:|
| FROM_UNIXTIME  |
| UNIX_TIMESTAMP |

| Oracle |
|:------:|
|  NVL   |

### Line Limit

If there is a limit keyword in the query, Doris will translate it into semantics suitable for different data sources.

## Write Data

After the JDBC catalog is established in Doris, you can write data directly through the insert into statement, or write the results of the query executed by Doris into the JDBC catalog, or import data from one JDBC catalog to another JDBC catalog.

### Example

```sql
insert into mysql_catalog.mysql_database.mysql_table values(1, "doris");
insert into mysql_catalog.mysql_database.mysql_table select * from table;
```

### Transaction

In Doris, data is written to External Tables in batches. If the ingestion process is interrupted, rollbacks might be required. That's why JDBC catalog tables support data writing transactions. You can utilize this feature by setting the session variable: `enable_odbc_transcation `.

```
set enable_odbc_transcation = true; 
```

The transaction mechanism ensures the atomicity of data writing to JDBC External Tables, but it reduces performance to a certain extent. You may decide whether to enable transactions based on your own tradeoff.

## JDBC Connection Pool Configuration

In Doris, each Frontend (FE) and Backend (BE) node maintains a connection pool, thus avoiding the need to frequently open and close individual connections to data sources. Each connection within this pool can be used to establish a connection to a data source and perform queries. After operations are completed, connections are returned to the pool for reuse. This not only enhances performance but also reduces system load during connection establishment, and helps to prevent hitting the maximum connection limits of the data sources.

The following catalog configuration properties are available for tuning the behavior of the connection pool:

| Parameter Name                  | Default Value  | Description and Behavior                                                                                                                                                                                                                                                                                                                                                                          |
|---------------------------------|----------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `connection_pool_min_size`      | 1              | Defines the minimum number of connections that the pool will maintain, ensuring that this number of connections remains active when the keep-alive mechanism is enabled.                                                                                                                                                                                                                          |
| `connection_pool_max_size`      | 10             | Specifies the maximum number of connections allowed in the pool. Each catalog corresponding to every FE or BE node can hold up to this number of connections.                                                                                                                                                                                                                                     |
| `connection_pool_max_wait_time` | 5000           | Determines the maximum amount of time, in milliseconds, that the client will wait for a connection from the pool if none is immediately available.                                                                                                                                                                                                                                                |
| `connection_pool_max_life_time` | 1800000        | Sets the maximum lifetime of connections in the pool, in milliseconds. Connections exceeding this set time limit will be forcibly closed. Additionally, half of this value is used as the minimum evictable idle time for the pool. Connections reaching this idle time are considered for eviction, and the eviction task runs at intervals of one-tenth of the `connection_pool_max_life_time`. |
| `connection_pool_keep_alive`    | false          | Effective only on BE nodes, it controls whether to keep connections that have reached the minimum evictable idle time but not the maximum lifetime active. It is kept false by default to avoid unnecessary resource usage.                                                                                                                                                                       |

To prevent an accumulation of unused connection pool caches on the BE, the BE `jdbc_connection_pool_cache_clear_time_sec` parameter for the BE can be set to specify the interval for clearing the cache. With a default value of 28800 seconds (8 hours), the BE will forcibly clear all connection pool caches that have not been used beyond this interval.

## Guide

### View the JDBC catalog

You can query all catalogs in the current Doris cluster through SHOW CATALOGS:

```sql
SHOW CATALOGS;
```

Query the creation statement of a catalog through SHOW CREATE CATALOG:

```sql
SHOW CREATE CATALOG <catalog_name>;
```

### Drop the JDBC catalog

A catalog can be deleted via DROP CATALOG:

```sql
DROP CATALOG <catalog_name>;
```

### Query the JDBC catalog

1. Use SWITCH to switch the catalog in effect for the current session:

    ```sql
    SWITCH <catalog_name>;
    ```

2. Query all libraries under the current catalog through SHOW DATABASES:

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

4. Query all tables under the current catalog through SHOW TABLES:

    ```sql
    SHOW TABLES FROM <catalog_name>.<database_name>;
    ```

    ```sql
    SHOW TABLES FROM <database_name>;
    ```

    ```sql
    SHOW TABLES;
    ```

5. Query the data of a table under the current catalog through SELECT:

    ```sql
    SELECT * FROM <table_name>;
    ```

## SQL Passthrough

In versions prior to Doris 2.0.3, users could only perform query operations (SELECT) through the JDBC catalog.
Starting from version Doris 2.0.4, users can perform DDL (Data Definition Language) and DML (Data Manipulation Language) operations on JDBC data sources using the `CALL` command.

```
CALL EXECUTE_STMT("catalog_name", "raw_stmt_string");
```

The `EXECUTE_STMT()` procedure involves two parameters:

- Catalog Name: Currently, only the JDBC catalog is supported.
- Execution Statement: Currently, only DDL and DML statements are supported. These statements must use the syntax specific to the JDBC data source.

```
CALL EXECUTE_STMT("jdbc_catalog", "insert into db1.tbl1 values(1,2), (3, 4)");

CALL EXECUTE_STMT("jdbc_catalog", "delete from db1.tbl1 where k1 = 2");

CALL EXECUTE_STMT("jdbc_catalog", "create table db1.tbl2 (k1 int)");
```

### Principles and Limitations

Through the `CALL EXECUTE_STMT()` command, Doris directly sends the SQL statements written by the user to the JDBC data source associated with the catalog for execution. Therefore, this operation has the following limitations:

- The SQL statements must be in the syntax specific to the data source, as Doris does not perform syntax and semantic checks.
- It is recommended that table names in SQL statements be fully qualified, i.e., in the `db.tbl` format. If the `db` is not specified, the db name specified in the JDBC catalog JDBC URL will be used.
- SQL statements cannot reference tables outside of the JDBC data source, nor can they reference Doris's tables. However, they can reference tables within the JDBC data source that have not been synchronized to the Doris JDBC catalog.
- When executing DML statements, it is not possible to obtain the number of rows inserted, updated, or deleted; success of the command execution can only be confirmed.
- Only users with LOAD permissions on the catalog can execute this command.

## Supported Datasoures

### MySQL

#### Example

* mysql 5.7

```sql
CREATE CATALOG jdbc_mysql PROPERTIES (
    "type" = "jdbc",
    "user" = "root",
    "password" = "123456",
    "jdbc_url" = "jdbc:mysql://127.0.0.1:3306/demo",
    "driver_url" = "mysql-connector-java-5.1.47.jar",
    "driver_class" = "com.mysql.jdbc.Driver"
)
```

* mysql 8

```sql
CREATE CATALOG jdbc_mysql PROPERTIES (
    "type" = "jdbc",
    "user" = "root",
    "password" = "123456",
    "jdbc_url" = "jdbc:mysql://127.0.0.1:3306/demo",
    "driver_url" = "mysql-connector-java-8.0.25.jar",
    "driver_class" = "com.mysql.cj.jdbc.Driver"
)
```

#### Hierarchy Mapping

|  Doris   |    MySQL     |
|:--------:|:------------:|
| Catalog  | MySQL Server |
| Database | Database     |
| Table    | Table        |

#### Type Mapping

| MYSQL Type                                | Doris Type              | Comment                                                                                |
|-------------------------------------------|-------------------------|----------------------------------------------------------------------------------------|
| BOOLEAN                                   | TINYINT                 |                                                                                        |
| TINYINT                                   | TINYINT                 |                                                                                        |
| SMALLINT                                  | SMALLINT                |                                                                                        |
| MEDIUMINT                                 | INT                     |                                                                                        |
| INT                                       | INT                     |                                                                                        |
| BIGINT                                    | BIGINT                  |                                                                                        |
| UNSIGNED TINYINT                          | SMALLINT                | Doris does not have an UNSIGNED data type, so expand by an order of magnitude          |
| UNSIGNED MEDIUMINT                        | INT                     | Doris does not have an UNSIGNED data type, so expand by an order of magnitude          |
| UNSIGNED INT                              | BIGINT                  | Doris does not have an UNSIGNED data type, so expand by an order of magnitude          |
| UNSIGNED BIGINT                           | LARGEINT                |                                                                                        |
| FLOAT                                     | FLOAT                   |                                                                                        |
| DOUBLE                                    | DOUBLE                  |                                                                                        |
| DECIMAL                                   | DECIMAL                 |                                                                                        |
| UNSIGNED DECIMAL(p,s)                     | DECIMAL(p+1,s) / STRING | If p+1>38, the Doris STRING type will be used.                                         |
| DATE                                      | DATE                    |                                                                                        |
| TIMESTAMP                                 | DATETIME                |                                                                                        |
| DATETIME                                  | DATETIME                |                                                                                        |
| YEAR                                      | SMALLINT                |                                                                                        |
| TIME                                      | STRING                  |                                                                                        |
| CHAR                                      | CHAR                    |                                                                                        |
| VARCHAR                                   | VARCHAR                 |                                                                                        |
| JSON                                      | STRING                  | For better performance, map JSON from external data sources to STRING instead of JSONB |
| SET                                       | STRING                  |                                                                                        |
| BIT                                       | BOOLEAN/STRING          | BIT(1) will be mapped to BOOLEAN, and other BITs will be mapped to STRING              |
| TINYTEXT、TEXT、MEDIUMTEXT、LONGTEXT         | STRING                  |                                                                                        |
| BLOB、MEDIUMBLOB、LONGBLOB、TINYBLOB         | STRING                  |                                                                                        |
| TINYSTRING、STRING、MEDIUMSTRING、LONGSTRING | STRING                  |                                                                                        |
| BINARY、VARBINARY                          | STRING                  |                                                                                        |
| Other                                     | UNSUPPORTED             |                                                                                        |

### PostgreSQL

#### Example

```sql
CREATE CATALOG jdbc_postgresql PROPERTIES (
    "type" = "jdbc",
    "user" = "root",
    "password" = "123456",
    "jdbc_url" = "jdbc:postgresql://127.0.0.1:5432/demo",
    "driver_url" = "postgresql-42.5.1.jar",
    "driver_class" = "org.postgresql.Driver"
);
```

#### Hierarchy Mapping

As for data mapping from PostgreSQL to Doris, one Database in Doris corresponds to one schema in the specified database in PostgreSQL (for example, "demo" in `jdbc_url`  above), and one Table in that Database corresponds to one table in that schema. To make it more intuitive, the mapping relations are as follows:

|  Doris   | PostgreSQL |
|:--------:|:----------:|
| Catalog  | Database   |
| Database | Schema     |
| Table    | Table      |

:::tip
Doris obtains all schemas that PG user can access through the SQL statement: `select nspname from pg_namespace where has_schema_privilege('<UserName>', nspname, 'USAGE');` and map these schemas to doris database.   
:::

#### Type Mapping

 | POSTGRESQL Type                         | Doris Type     | Comment                                                                                |
 |-----------------------------------------|----------------|----------------------------------------------------------------------------------------|
 | boolean                                 | BOOLEAN        |                                                                                        |
 | smallint/int2                           | SMALLINT       |                                                                                        |
 | integer/int4                            | INT            |                                                                                        |
 | bigint/int8                             | BIGINT         |                                                                                        |
 | decimal/numeric                         | DECIMAL        |                                                                                        |
 | real/float4                             | FLOAT          |                                                                                        |
 | double precision                        | DOUBLE         |                                                                                        |
 | smallserial                             | SMALLINT       |                                                                                        |
 | serial                                  | INT            |                                                                                        |
 | bigserial                               | BIGINT         |                                                                                        |
 | char                                    | CHAR           |                                                                                        |
 | varchar/text                            | STRING         |                                                                                        |
 | timestamp                               | DATETIME       |                                                                                        |
 | date                                    | DATE           |                                                                                        |
 | json/jsonb                              | STRING         | For better performance, map JSON from external data sources to STRING instead of JSONB |
 | time                                    | STRING         |                                                                                        |
 | interval                                | STRING         |                                                                                        |
 | point/line/lseg/box/path/polygon/circle | STRING         |                                                                                        |
 | cidr/inet/macaddr                       | STRING         |                                                                                        |
 | bit                                     | BOOLEAN/STRING | bit(1) will be mapped to BOOLEAN, and other bits will be mapped to STRING              |
 | uuid                                    | STRING         |                                                                                        |
 | Other                                   | UNSUPPORTED    |                                                                                        |

### Oracle

#### Example

```sql
CREATE CATALOG jdbc_oracle PROPERTIES (
    "type" = "jdbc",
    "user" = "root",
    "password" = "123456",
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
| Database | User     |
| Table    | Table    |

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
    "type" = "jdbc",
    "user" = "SA",
    "password" = "Doris123456",
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
| Database | Schema    |
| Table    | Table     |

#### Type Mapping

| SQLServer Type                         | Doris Type    | Comment                                                                          |
|----------------------------------------|---------------|----------------------------------------------------------------------------------|
| bit                                    | BOOLEAN       |                                                                                  |
| tinyint                                | SMALLINT      | SQLServer's tinyint is an unsigned number, so it maps to Doris' SMALLINT         |
| smallint                               | SMALLINT      |                                                                                  |
| int                                    | INT           |                                                                                  |
| bigint                                 | BIGINT        |                                                                                  |
| real                                   | FLOAT         |                                                                                  |
| float                                  | DOUBLE        |                                                                                  |
| money                                  | DECIMAL(19,4) |                                                                                  |
| smallmoney                             | DECIMAL(10,4) |                                                                                  |
| decimal/numeric                        | DECIMAL       |                                                                                  |
| date                                   | DATE          |                                                                                  |
| datetime/datetime2/smalldatetime       | DATETIMEV2    |                                                                                  |
| char/varchar/text/nchar/nvarchar/ntext | STRING        |                                                                                  |
| time/datetimeoffset                    | STRING        |                                                                                  |
| timestamp                              | STRING        | Read the hexadecimal display of binary data, which has no practical significance.|
| Other                                  | UNSUPPORTED   |                                                                                  |

### Doris

The JDBC catalog supports connecting to another Doris database:

* MySQL 5.7 driver

```sql
CREATE CATALOG jdbc_doris PROPERTIES (
    "type" = "jdbc",
    "user" = "root",
    "password" = "123456",
    "jdbc_url" = "jdbc:mysql://127.0.0.1:9030?useSSL=false",
    "driver_url" = "mysql-connector-java-5.1.47.jar",
    "driver_class" = "com.mysql.jdbc.Driver"
)
```

* MySQL 8 driver

```sql
CREATE CATALOG jdbc_doris PROPERTIES (
    "type" = "jdbc",
    "user" = "root",
    "password" = "123456",
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
    "type" = "jdbc",
    "user" = "root",
    "password" = "123456",
    "jdbc_url" = "jdbc:clickhouse://127.0.0.1:8123/demo",
    "driver_url" = "clickhouse-jdbc-0.4.2-all.jar",
    "driver_class" = "com.clickhouse.jdbc.ClickHouseDriver"
);
```

#### Hierarchy Mapping

|  Doris   |    ClickHouse     |
|:--------:|:-----------------:|
| Catalog  | ClickHouse Server |
| Database | Database          |
| Table    | Table             |

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
    "type" = "jdbc",
    "user" = "SYSTEM",
    "password" = "SAPHANA",
    "jdbc_url" = "jdbc:sap://localhost:31515/TEST",
    "driver_url" = "ngdbc.jar",
    "driver_class" = "com.sap.db.jdbc.Driver"
)
```

#### Hierarchy Mapping

|  Doris   | SAP HANA |
|:--------:|:--------:|
| Catalog  | Database |
| Database | Schema  |
| Table    | Table   |

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
    "type" = "jdbc",
    "user" = "hadoop",
    "password" = "",
    "jdbc_url" = "jdbc:trino://localhost:9000/hive",
    "driver_url" = "trino-jdbc-389.jar",
    "driver_class" = "io.trino.jdbc.TrinoDriver"
);
```

* Presto

```sql
CREATE CATALOG jdbc_presto PROPERTIES (
    "type" = "jdbc",
    "user" = "hadoop",
    "password" = "",
    "jdbc_url" = "jdbc:presto://localhost:9000/hive",
    "driver_url" = "presto-jdbc-0.280.jar",
    "driver_class" = "com.facebook.presto.jdbc.PrestoDriver"
);
```

#### Hierarchy Mapping

When mapping Trino, Doris's Database corresponds to a Schema under the specified catalog in Trino (such as "hive" in the `jdbc_url` parameter in the example). The Table under the Database of Doris corresponds to the Tables under the Schema in Trino. That is, the mapping relationship is as follows:

|  Doris   | Trino/Presto |
|:--------:|:------------:|
| Catalog  | Catalog      |
| Database | Schema       |
| Table    | Table        |


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
    "type" = "jdbc",
    "user" = "root",
    "password" = "123456",
    "jdbc_url" = "jdbc:oceanbase://127.0.0.1:2881/demo",
    "driver_url" = "oceanbase-client-2.4.2.jar",
    "driver_class" = "com.oceanbase.jdbc.Driver"
)
```

:::tip
When Doris connects to OceanBase, it will automatically recognize that OceanBase is in MySQL or Oracle mode. Hierarchical correspondence and type mapping refer to [MySQL](#mysql) and [Oracle](#oracle)
:::

### DB2

#### Example

```sql
CREATE CATALOG `jdbc_db2` PROPERTIES (
    "user" = "db2inst1",
    "type" = "jdbc",
    "password" = "123456",
    "jdbc_url" = "jdbc:db2://127.0.0.1:50000/doris",
    "driver_url" = "jcc-11.5.8.0.jar",
    "driver_class" = "com.ibm.db2.jcc.DB2Driver"
);
```

#### Hierarchy Mapping

When mapping DB2, Doris's Database corresponds to a Schema under the specified DataBase in DB2 (such as "doris" in the `jdbc_url` parameter in the example). The Table under Doris' Database corresponds to the Tables under Schema in DB2. That is, the mapping relationship is as follows:

|  Doris   |   DB2    |
|:--------:|:--------:|
| Catalog  | DataBase |
| Database |  Schema  |
|  Table   |  Table   |


#### Type Mapping

| DB2 Type         | Trino Type   | Notes |
|------------------|--------------|-------|
| SMALLINT         | SMALLINT     |       |
| INT              | INT          |       |
| BIGINT           | BIGINT       |       |
| DOUBLE           | DOUBLE       |       |
| DOUBLE PRECISION | DOUBLE       |       |
| FLOAT            | DOUBLE       |       |
| REAL             | FLOAT        |       |
| NUMERIC          | DECIMAL      |       |
| DECIMAL          | DECIMAL      |       |
| DECFLOAT         | DECIMAL      |       |
| DATE             | DATE         |       |
| TIMESTAMP        | DATETIME     |       |
| CHAR             | CHAR         |       |
| CHAR VARYING     | VARCHAR      |       |
| VARCHAR          | VARCHAR      |       |
| LONG VARCHAR     | VARCHAR      |       |
| VARGRAPHIC       | STRING       |       |
| LONG VARGRAPHIC  | STRING       |       |
| TIME             | STRING       |       |
| CLOB             | STRING       |       |
| OTHER            | UNSUPPORTED  |       |

## JDBC Drivers

It is recommended to use the following versions of Driver to connect to the corresponding database. Other versions of the Driver have not been tested and may cause unexpected problems.

|    Source    |                        JDBC Driver Version                        |
|:------------:|:-----------------------------------------------------------------:|
| MySQL 5.x    |                  mysql-connector-java-5.1.47.jar                  |
|  MySQL 8.x   |                  mysql-connector-java-8.0.25.jar                  |
|  PostgreSQL  |                       postgresql-42.5.1.jar                       |
|    Oracle    |                            ojdbc8.jar                             |
|  SQLServer   |                    mssql-jdbc-11.2.3.jre8.jar                     |
|    Doris     | mysql-connector-java-5.1.47.jar / mysql-connector-java-8.0.25.jar |
|  Clickhouse  |                   clickhouse-jdbc-0.4.2-all.jar                   |
|   SAP HAHA   |                             ngdbc.jar                             |
| Trino/Presto |            trino-jdbc-389.jar / presto-jdbc-0.280.jar             |
|  OceanBase   |                    oceanbase-client-2.4.2.jar                     |
|     DB2      |                         jcc-11.5.8.0.jar                          |

## FAQ

1. In addition to MySQL, Oracle, PostgreSQL, SQLServer, ClickHouse, SAP HANA, Trino/Presto, OceanBase, DB2, whether it can support more databases

   Currently, Doris is only adapted to MySQL, Oracle, PostgreSQL, SQLServer, ClickHouse, SAP HANA, Trino/Presto, OceanBase, and DB2. Adaptation work for other databases is under planning. In principle, any database that supports JDBC access can Can be accessed through JDBC appearance. If you need to access other surfaces, you are welcome to modify the code and contribute to Doris.

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

    You need to add `zeroDateTimeBehavior=convertToNull` to the end of the JDBC connection string when creating the catalog `jdbc_url`, such as `"jdbc_url" = "jdbc:mysql://127.0.0.1:3306/test?zeroDateTimeBehavior=convertToNull"`
    In this case, JDBC will convert 0000-00-00 or 0000-00-00 00:00:00 into null, and then Doris will process all Date/DateTime type columns in the current catalog as nullable types, so that It can be read normally.

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

   You can add `?useSSL=false` to the end of the JDBC connection string when creating the catalog, such as `"jdbc_url" = "jdbc:mysql://127.0.0.1:3306/test?useSSL=false"`

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

   You can add `encrypt=false` to the end of the JDBC connection string when creating the catalog, such as `"jdbc_url" = "jdbc:sqlserver://127.0.0.1:1433;DataBaseName=doris_test;encrypt=false"`

9. `Non supported character set (add orai18n.jar in your classpath): ZHS16GBK` exception occurs when reading Oracle

    Download [orai18n.jar](https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html) and put it in the lib directory of Doris FE and the lib/java_extensions directory of BE (versions before Doris 2.0 It needs to be placed in the lib directory of BE).

    Starting from version 2.0.2, this file can be placed in the `custom_lib/` directory of FE and BE (if it does not exist, just create it manually) to prevent the file from being lost when the lib directory is replaced when upgrading the cluster.

10. `NoClassDefFoundError: net/jpountz/lz4/LZ4Factory` error message appears when reading Clickhouse data through jdbc catalog

    You can download the [lz4-1.3.0.jar](https://repo1.maven.org/maven2/net/jpountz/lz4/lz4/1.3.0/lz4-1.3.0.jar) package first, and then put it in DorisFE lib directory and BE's `lib/lib/java_extensions` directory (versions before Doris 2.0 need to be placed in BE's lib directory).

    Starting from version 2.0.2, this file can be placed in the `custom_lib/` directory of FE and BE (if it does not exist, just create it manually) to prevent the file from being lost due to the replacement of the lib directory when upgrading the cluster.

11. If there is a prolonged delay or no response when querying MySQL through JDBC catalog, or if it hangs for an extended period and a significant number of "write lock" logs appear in the fe.warn.log, consider adding a socketTimeout parameter to the URL. For example: `jdbc:mysql://host:port/database?socketTimeout=30000`. This prevents the JDBC client from waiting indefinitely after MySQL closes the connection.
