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

1. Supported datas sources include MySQL, PostgreSQL, Oracle, SQLServer, Clickhouse, Doris, SAP HANA, Trino and OceanBase.

## Create Catalog

1. MySQL

<version since="1.2.0"></version>

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

2. PostgreSQL

<version since="1.2.2"></version>

   ```sql
   CREATE CATALOG jdbc_postgresql PROPERTIES (
       "type"="jdbc",
       "user"="root",
       "password"="123456",
       "jdbc_url" = "jdbc:postgresql://127.0.0.1:5449/demo",
       "driver_url" = "postgresql-42.5.1.jar",
       "driver_class" = "org.postgresql.Driver"
   );
   ```
> Doris obtains all schemas that PG user can access through the SQL statement: `select nspname from pg_namespace where has_schema_privilege('<UserName>', nspname, 'USAGE');` and map these schemas to doris database.   

   As for data mapping from PostgreSQL to Doris, one Database in Doris corresponds to one schema in the specified database in PostgreSQL (for example, "demo" in `jdbc_url`  above), and one Table in that Database corresponds to one table in that schema. To make it more intuitive, the mapping relations are as follows:

| Doris    | PostgreSQL |
| -------- | ---------- |
| Catalog  | Database   |
| Database | Schema     |
| Table    | Table      |

3. Oracle

<version since="1.2.2"></version>

   ```sql
   CREATE CATALOG jdbc_oracle PROPERTIES (
       "type"="jdbc",
       "user"="root",
       "password"="123456",
       "jdbc_url" = "jdbc:oracle:thin:@127.0.0.1:1521:helowin",
       "driver_url" = "ojdbc6.jar",
       "driver_class" = "oracle.jdbc.driver.OracleDriver"
   );
   ```

   As for data mapping from Oracle to Doris, one Database in Doris corresponds to one User, and one Table in that Database corresponds to one table that the User has access to. In conclusion, the mapping relations are as follows:

| Doris    | Oracle |
| -------- | ---------- |
| Catalog  | Database   |
| Database | User       |
| Table    | Table      |

4. Clickhouse

<version since="1.2.2"></version>

   ```sql
   CREATE CATALOG jdbc_clickhouse PROPERTIES (
       "type"="jdbc",
       "user"="root",
       "password"="123456",
       "jdbc_url" = "jdbc:clickhouse://127.0.0.1:8123/demo",
       "driver_url" = "clickhouse-jdbc-0.3.2-patch11-all.jar",
       "driver_class" = "com.clickhouse.jdbc.ClickHouseDriver"
   );
   ```

5. SQLServer

<version since="1.2.2"></version>

   ```sql
   CREATE CATALOG sqlserver_catalog PROPERTIES (
   	"type"="jdbc",
   	"user"="SA",
   	"password"="Doris123456",
   	"jdbc_url" = "jdbc:sqlserver://localhost:1433;DataBaseName=doris_test",
   	"driver_url" = "mssql-jdbc-11.2.3.jre8.jar",
   	"driver_class" = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
   );
   ```

   As for data mapping from SQLServer to Doris, one Database in Doris corresponds to one schema in the specified database in SQLServer (for example, "doris_test" in `jdbc_url`  above), and one Table in that Database corresponds to one table in that schema. The mapping relations are as follows:

| Doris    | SQLServer |
| -------- | --------- |
| Catalog  | Database  |
| Database | Schema    |
| Table    | Table     |
    
6. Doris

<version since="1.2.3"></version>

Jdbc Catalog also support to connect another Doris database:

```sql
CREATE CATALOG doris_catalog PROPERTIES (
    "type"="jdbc",
    "user"="root",
    "password"="123456",
    "jdbc_url" = "jdbc:mysql://127.0.0.1:9030?useSSL=false",
    "driver_url" = "mysql-connector-java-5.1.47.jar",
    "driver_class" = "com.mysql.jdbc.Driver"
);
```

Currently, Jdbc Catalog only support to use 5.x version of JDBC jar package to connect another Doris database. If you use 8.x version of JDBC jar package, the data type of column may not be matched.

7. SAP_HANA

<version since="1.2.3"></version>

```sql
CREATE CATALOG hana_catalog PROPERTIES (
    "type"="jdbc",
    "user"="SYSTEM",
    "password"="SAPHANA",
    "jdbc_url" = "jdbc:sap://localhost:31515/TEST",
    "driver_url" = "ngdbc.jar",
    "driver_class" = "com.sap.db.jdbc.Driver"
)
```

| Doris    | SAP_HANA |
|----------|----------|
| Catalog  | Database | 
| Database | Schema   |
| Table    | Table    |

8. Trino

<version since="1.2.4"></version>

```sql
CREATE CATALOG trino_catalog PROPERTIES (
    "type"="jdbc",
    "user"="hadoop",
    "password"="",
    "jdbc_url" = "jdbc:trino://localhost:9000/hive",
    "driver_url" = "trino-jdbc-389.jar",
    "driver_class" = "io.trino.jdbc.TrinoDriver"
);
```

When Trino is mapped, Doris's Database corresponds to a Schema in Trino that specifies Catalog (such as "hive" in the 'jdbc_url' parameter in the example). The Table in Doris's Database corresponds to the Tables in Trino's Schema. That is, the mapping relationship is as follows:

| Doris    | Trino   |
|----------|---------|
| Catalog  | Catalog | 
| Database | Schema  |
| Table    | Table   |

9. OceanBase

<version since="dev"></version>

```sql
CREATE CATALOG jdbc_oceanbase_mysql PROPERTIES (
    "type"="jdbc",
    "user"="root",
    "password"="123456",
    "jdbc_url" = "jdbc:oceanbase://127.0.0.1:2881/demo",
    "driver_url" = "oceanbase-client-2.4.2.jar",
    "driver_class" = "com.oceanbase.jdbc.Drive",
    "oceanbase_mode" = "mysql"
)

CREATE CATALOG jdbc_oceanbase_oracle PROPERTIES (
    "type"="jdbc",
    "user"="root",
    "password"="123456",
    "jdbc_url" = "jdbc:oceanbase://127.0.0.1:2881/demo",
    "driver_url" = "oceanbase-client-2.4.2.jar",
    "driver_class" = "com.oceanbase.jdbc.Drive",
    "oceanbase_mode" = "oracle"
)
```

### Parameter Description

| Parameter                 | Required or Not | Default Value | Description                                        |
|---------------------------|-----------------|---------------| -------------------------------------------------- |
| `user`                    | Yes             |               | Username in relation to the corresponding database |
| `password`                | Yes             |               | Password for the corresponding database            |
| `jdbc_url `               | Yes             |               | JDBC connection string                             |
| `driver_url `             | Yes             |               | JDBC Driver Jar                                    |
| `driver_class `           | Yes             |               | JDBC Driver Class                                  |
| `only_specified_database` | No              | "false"       | Whether only the database specified to be synchronized.                                  |
| `lower_case_table_names`  | No              | "false"       | Whether to synchronize jdbc external data source table names in lower case. |
| `specified_database_list` | No              | ""            | When only_specified_database=true，only synchronize the specified databases. split with ','. db name is case sensitive.|
| `oceanbase_mode`          | No              | ""            | When the connected external data source is OceanBase, the mode must be specified as mysql or oracle                        |
> `driver_url` can be specified in three ways:
>
> 1. File name. For example,  `mysql-connector-java-5.1.47.jar`. Please place the Jar file package in  `jdbc_drivers/`  under the FE/BE deployment directory in advance so the system can locate the file. You can change the location of the file by modifying  `jdbc_drivers_dir`  in fe.conf and be.conf.
>
> 2. Local absolute path. For example, `file:///path/to/mysql-connector-java-5.1.47.jar`. Please place the Jar file package in the specified paths of FE/BE node.
>
> 3. HTTP address. For example, `https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com/jdbc_driver/mysql-connector-java-5.1.47.jar`. The system will download the Driver file from the HTTP address. This only supports HTTP services with no authentication requirements.

> `only_specified_database`:
>
> When the JDBC is connected, you can specify which database/schema to connect. For example, you can specify the DataBase in mysql `jdbc_url`; you can specify the CurrentSchema in PG `jdbc_url`. When `only_specified_database=true` and `specified_database_list` is empty, only the database in jdbc_url specified to be synchronized. When `only_specified_database=true` and `specified_database_list` with some database names，and these names will specified to be synchronized。
> 
> If you connect the Oracle database when using this property, please  use the version of the jar package above 8 or more (such as ojdbc8.jar).


## Query

```
select * from mysql_catalog.mysql_database.mysql_table where k1 > 1000 and k3 ='term';
```

In some cases, the keywords in the database might be used as the field names. For queries to function normally in these cases, Doris will add escape characters to the field names and tables names in SQL statements based on the rules of different databases, such as (``) for MySQL, ([]) for SQLServer, and ("") for PostgreSQL and Oracle. This might require extra attention on case sensitivity. You can view the query statements sent to these various databases via ```explain sql```.

## Write Data

<version since="1.2.2">
After creating a JDBC Catalog in Doris, you can write data or query results to it using the `insert into` statement. You can also ingest data from one JDBC Catalog Table to another JDBC Catalog Table.
</version>

Example:

```
insert into mysql_catalog.mysql_database.mysql_table values(1, "doris");
insert into mysql_catalog.mysql_database.mysql_table select * from table;
```

### Transaction

In Doris, data is written to External Tables in batches. If the ingestion process is interrupted, rollbacks might be required. That's why JDBC Catalog Tables support data writing transactions. You can utilize this feature by setting the session variable: `enable_odbc_transcation `.

```
set enable_odbc_transcation = true; 
```

The transaction mechanism ensures the atomicity of data writing to JDBC External Tables, but it reduces performance to a certain extent. You may decide whether to enable transactions based on your own tradeoff.

## Column Type Mapping

### MySQL

| MYSQL Type                                                   | Doris Type  | Comment                                                      |
| ------------------------------------------------------------ | ----------- | ------------------------------------------------------------ |
| BOOLEAN                                                      | BOOLEAN     |                                                              |
| TINYINT                                                      | TINYINT     |                                                              |
| SMALLINT                                                     | SMALLINT    |                                                              |
| MEDIUMINT                                                    | INT         |                                                              |
| INT                                                          | INT         |                                                              |
| BIGINT                                                       | BIGINT      |                                                              |
| UNSIGNED TINYINT                                             | SMALLINT    | Doris does not support UNSIGNED data types so UNSIGNED TINYINT will be mapped to SMALLINT. |
| UNSIGNED MEDIUMINT                                           | INT         | Doris does not support UNSIGNED data types so UNSIGNED MEDIUMINT will be mapped to INT. |
| UNSIGNED INT                                                 | BIGINT      | Doris does not support UNSIGNED data types so UNSIGNED INT will be mapped to BIGINT. |
| UNSIGNED BIGINT                                              | LARGEINT      |                                                              |
| FLOAT                                                        | FLOAT       |                                                              |
| DOUBLE                                                       | DOUBLE      |                                                              |
| DECIMAL                                                      | DECIMAL     |                                                              |
| DATE                                                         | DATE        |                                                              |
| TIMESTAMP                                                    | DATETIME    |                                                              |
| DATETIME                                                     | DATETIME    |                                                              |
| YEAR                                                         | SMALLINT    |                                                              |
| TIME                                                         | STRING      |                                                              |
| CHAR                                                         | CHAR        |                                                              |
| VARCHAR                                                      | VARCHAR      |                                                              |
| TINYTEXT、TEXT、MEDIUMTEXT、LONGTEXT、TINYBLOB、BLOB、MEDIUMBLOB、LONGBLOB、TINYSTRING、STRING、MEDIUMSTRING、LONGSTRING、BINARY、VARBINARY、JSON、SET、BIT | STRING      |                                                              |
| Other                                                        | UNSUPPORTED |                                                              |

### PostgreSQL

 POSTGRESQL Type | Doris Type | Comment |
|---|---|---|
| boolean | BOOLEAN | |
| smallint/int2 | SMALLINT | |
| integer/int4 | INT | |
| bigint/int8 | BIGINT | |
| decimal/numeric | DECIMAL | |
| real/float4 | FLOAT | |
| double precision | DOUBLE | |
| smallserial | SMALLINT | |
| serial | INT | |
| bigserial | BIGINT | |
| char | CHAR | |
| varchar/text | STRING | |
| timestamp | DATETIME | |
| date | DATE | |
| time | STRING | |
| interval | STRING | |
| point/line/lseg/box/path/polygon/circle | STRING | |
| cidr/inet/macaddr | STRING | |
| bit/bit(n)/bit varying(n) | STRING | `bit ` will be mapped to `STRING` in Doris. It will be read as `true/false` instead of `1/0` |
| uuid/josnb | STRING | |
|Other| UNSUPPORTED |

### Oracle

| ORACLE Type                   | Doris Type  | Comment                                                      |
| ----------------------------- | ----------- | ------------------------------------------------------------ |
| number(p) / number(p,0)       | TINYINT/SMALLINT/INT/BIGINT/LARGEINT | Doris will determine the type to map to based on the value of p: `p < 3` -> `TINYINT`; `p < 5` -> `SMALLINT`; `p < 10` -> `INT`; `p < 19` -> `BIGINT`; `p > 19` -> `LARGEINT` |
| number(p,s), [ if(s>0 && p>s) ] | DECIMAL(p,s) | |
| number(p,s), [ if(s>0 && p < s) ] | DECIMAL(s,s) |  |
| number(p,s), [ if(s<0) ] | TINYINT/SMALLINT/INT/BIGINT/LARGEINT | if s<0, Doris will set `p` to `p+|s|`, and perform the same mapping as `number(p) / number(p,0)`. |
| number |  | Doris does not support Oracle `NUMBER` type that does not specified p and s |
| float/real                    | DOUBLE      |                                                              |
| DATE                          | DATETIME    |                                                              |
| TIMESTAMP                     | DATETIME    |                                                              |
| CHAR/NCHAR                    | STRING      |                                                              |
| VARCHAR2/NVARCHAR2            | STRING      |                                                              |
| LONG/ RAW/ LONG RAW/ INTERVAL | STRING      |                                                              |
| Other                         | UNSUPPORTED |                                                              |

### SQLServer

| SQLServer Type                         | Doris Type  | Comment                                                      |
| -------------------------------------- | ----------- | ------------------------------------------------------------ |
| bit                                    | BOOLEAN     |                                                              |
| tinyint                                | SMALLINT    | The tinyint type in SQLServer is an unsigned number so it will be mapped to SMALLINT in Doris. |
| smallint                               | SMALLINT    |                                                              |
| int                                    | INT         |                                                              |
| bigint                                 | BIGINT      |                                                              |
| real                                   | FLOAT       |                                                              |
| float/money/smallmoney                 | DOUBLE      |                                                              |
| decimal/numeric                        | DECIMAL     |                                                              |
| date                                   | DATE        |                                                              |
| datetime/datetime2/smalldatetime       | DATETIMEV2  |                                                              |
| char/varchar/text/nchar/nvarchar/ntext | STRING      |                                                              |
| binary/varbinary                       | STRING      |                                                              |
| time/datetimeoffset                    | STRING      |                                                              |
| Other                                  | UNSUPPORTED |                                                              |


### Clickhouse

| ClickHouse Type                                      | Doris Type               | Comment                                                                                                                              |
|------------------------------------------------------|--------------------------|--------------------------------------------------------------------------------------------------------------------------------------|
| Bool                                                 | BOOLEAN                  |                                                                                                                                      |
| String                                               | STRING                   |                                                                                                                                      |
| Date/Date32                                          | DATEV2                   | JDBC CATLOG uses Datev2 type default when connecting ClickHouse                                                                      |
| DateTime/DateTime64                                  | DATETIMEV2               | JDBC CATLOG uses DateTimev2 type default when connecting ClickHouse                                                                  |
| Float32                                              | FLOAT                    |                                                                                                                                      |
| Float64                                              | DOUBLE                   |                                                                                                                                      |
| Int8                                                 | TINYINT                  |                                                                                                                                      |
| Int16/UInt8                                          | SMALLINT                 | Doris does not support UNSIGNED data types so UInt8 will be mapped to SMALLINT.                                                      |
| Int32/UInt16                                         | INT                      | Doris does not support UNSIGNED data types so UInt16 will be mapped to INT.                                                          |
| Int64/Uint32                                         | BIGINT                   | Doris does not support UNSIGNED data types so UInt32 will be mapped to BIGINT.                                                       |
| Int128/UInt64                                        | LARGEINT                 | Doris does not support UNSIGNED data types so UInt64 will be mapped to LARGEINT.                                                     |
| Int256/UInt128/UInt256                               | STRING                   | Doris does not support data types of such orders of magnitude so these will be mapped to STRING.                                     |
| DECIMAL                                              | DECIMAL/DECIMALV3/STRING | The Data type is based on the DECIMAL field's (precision, scale) and the `enable_decimal_conversion` configuration.                  |
| Enum/IPv4/IPv6/UUID                                  | STRING                   | Data of IPv4 and IPv6 type will be displayed with an extra `/` as a prefix. To remove the `/`, you can use the `split_part`function. |
| <version since="dev" type="inline"> Array </version> | ARRAY                    | Array internal basic type adaptation logic refers to the preceding types. Nested types are not supported                             |
| Other                                                | UNSUPPORTED              |                                                                                                                                      |

### Doris

| Doris Type | Jdbc Catlog Doris Type | Comment |
|---|---|---|
| BOOLEAN | BOOLEAN | |
| TINYINT | TINYINT | |
| SMALLINT | SMALLINT | |
| INT | INT | |
| BIGINT | BIGINT | |
| LARGEINT | LARGEINT | |
| FLOAT | FLOAT | |
| DOUBLE | DOUBLE | |
| DECIMAL / DECIMALV3 | DECIMAL/DECIMALV3/STRING | The Data type is based on the DECIMAL field's (precision, scale) and the `enable_decimal_conversion` configuration |
| DATE | DATEV2 | JDBC CATLOG uses Datev2 type default when connecting DORIS |
| DATEV2 | DATEV2 |  |
| DATETIME | DATETIMEV2 | JDBC CATLOG uses DATETIMEV2 type default when connecting DORIS |
| DATETIMEV2 | DATETIMEV2 | |
| CHAR | CHAR | |
| VARCHAR | VARCHAR | |
| STRING | STRING | |
| TEXT | STRING | |
|Other| UNSUPPORTED |

### SAP HANA

| SAP HANA Type | Doris Type               | Comment                                                                                                            |
|---------------|--------------------------|--------------------------------------------------------------------------------------------------------------------|
| BOOLEAN       | BOOLEAN                  |                                                                                                                    |
| TINYINT       | TINYINT                  |                                                                                                                    |
| SMALLINT      | SMALLINT                 |                                                                                                                    |
| INTERGER      | INT                      |                                                                                                                    |
| BIGINT        | BIGINT                   |                                                                                                                    |
| SMALLDECIMAL  | DECIMALV3                |                                                                                                                    |
| DECIMAL       | DECIMAL/DECIMALV3/STRING | The Data type is based on the DECIMAL field's (precision, scale) and the `enable_decimal_conversion` configuration |
| REAL          | FLOAT                    |                                                                                                                    |
| DOUBLE        | DOUBLE                   |                                                                                                                    |
| DATE          | DATEV2                   | JDBC CATLOG uses Datev2 type default when connecting HANA                                                          |
| TIME          | TEXT                     |                                                                                                                    |
| TIMESTAMP     | DATETIMEV2               | JDBC CATLOG uses DATETIMEV2 type default when connecting HANA                                                      |
| SECONDDATE    | DATETIMEV2               | JDBC CATLOG uses DATETIMEV2 type default when connecting HANA                                                      |
| VARCHAR       | TEXT                     |                                                                                                                    |
| NVARCHAR      | TEXT                     |                                                                                                                    |
| ALPHANUM      | TEXT                     |                                                                                                                    |
| SHORTTEXT     | TEXT                     |                                                                                                                    |
| CHAR          | CHAR                     |                                                                                                                    |
| NCHAR         | CHAR                     |                                                                                                                    |
| Other         | UNSUPPORTED              |                                                                                                                    |

### Trino

| Trino Type                                           | Doris Type               | Comment                                                                                                            |
|------------------------------------------------------|--------------------------|--------------------------------------------------------------------------------------------------------------------|
| boolean                                              | BOOLEAN                  |                                                                                                                    |
| tinyint                                              | TINYINT                  |                                                                                                                    |
| smallint                                             | SMALLINT                 |                                                                                                                    |
| integer                                              | INT                      |                                                                                                                    |
| bigint                                               | BIGINT                   |                                                                                                                    |
| decimal                                              | DECIMAL/DECIMALV3/STRING | The Data type is based on the DECIMAL field's (precision, scale) and the `enable_decimal_conversion` configuration |
| real                                                 | FLOAT                    |                                                                                                                    |
| double                                               | DOUBLE                   |                                                                                                                    |
| date                                                 | DATE/DATEV2              | JDBC CATLOG uses Datev2 type default when connecting Trino                                                         |
| timestamp                                            | DATETIME/DATETIMEV2      | JDBC CATLOG uses DATETIMEV2 type default when connecting Trino                                                     |
| varchar                                              | TEXT                     |                                                                                                                    |
| char                                                 | CHAR                     |                                                                                                                    |
| <version since="dev" type="inline"> array </version> | ARRAY                    | Array internal basic type adaptation logic refers to the preceding types. Nested types are not supported           |
| others                                               | UNSUPPORTED              |                                                                                                                    |

**Note:**
Currently, only Hive connected to Trino has been tested. Other data sources connected to Trino have not been tested.

### OceanBase

For MySQL mode, please refer to [MySQL type mapping](#MySQL)
For Oracle mode, please refer to [Oracle type mapping](#Oracle)

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

3. Why does the error message "CAUSED BY: DataReadException: Zero date value prohibited" pop up when DateTime="0000:00:00 00:00:00" while reading MySQL external tables?

   This error occurs because of an illegal DateTime. It can be fixed by modifying the  `zeroDateTimeBehavior` parameter.

   The options for this parameter include: `EXCEPTION`,`CONVERT_TO_NULL`,`ROUND`. Respectively, they mean to report error, convert to null, and round the DateTime to "0001-01-01 00:00:00" when encountering an illegal DateTime.

   You can add `"jdbc_url"="jdbc:mysql://IP:PORT/doris_test?zeroDateTimeBehavior=convertToNull"` to the URL.

4. Why do loading failures happen when reading MySQL or other external tables?

   For example:

   ```
   failed to load driver class com.mysql.jdbc.driver in either of hikariconfig class loader
   ```

   Such errors occur because the `driver_class` has been wrongly put when creating the Resource. The problem with the above example is the letter case. It should be corrected as `"driver_class" = "com.mysql.jdbc.Driver"`.

5. How to fix communication link failures?

   If you run into the following errors:

   ```
   ERROR 1105 (HY000): errCode = 2, detailMessage = PoolInitializationException: Failed to initialize pool: Communications link failure
   
   The last packet successfully received from the server was 7 milliseconds ago.  The last packet sent successfully to the server was 4 milliseconds ago.
   CAUSED BY: CommunicationsException: Communications link failure
       
   The last packet successfully received from the server was 7 milliseconds ago.  The last packet sent successfully to the server was 4 milliseconds ago.
   CAUSED BY: SSLHandshakeExcepti
   ```

   Please check the be.out log of BE.

   If it contains the following message:

   ```
   WARN: Establishing SSL connection without server's identity verification is not recommended. 
   According to MySQL 5.5.45+, 5.6.26+ and 5.7.6+ requirements SSL connection must be established by default if explicit option isn't set. 
   For compliance with existing applications not using SSL the verifyServerCertificate property is set to 'false'. 
   You need either to explicitly disable SSL by setting useSSL=false, or set useSSL=true and provide truststore for server certificate verification.
   ```

   You can add `?useSSL=false` to the end of the JDBC connection string when creating Catalog. For example,  `"jdbc_url" = "jdbc:mysql://127.0.0.1:3306/test?useSSL=false"`.

6. What to do with the `OutOfMemoryError` when querying MySQL databases?

   To reduce memory usage, Doris obtains one batch of query results at a time, and has a size limit for each batch. However, MySQL conducts one-off loading of all query results by default, which means the "loading in batches" method won't work. To solve this, you need to specify "jdbc_url"="jdbc:mysql://IP:PORT/doris_test?useCursorFetch=true" in the URL.

7. What to do with errors such as "CAUSED BY: SQLException OutOfMemoryError" when performing JDBC queries?

   If you have set `useCursorFetch`  for MySQL, you can increase the JVM memory limit by modifying the value of `jvm_max_heap_size` in be.conf. The current default value is 1024M.

8. When using JDBC to query MySQL large data volume, if the query can occasionally succeed, occasionally report the following errors, and all the MySQL connections are completely disconnected when the error occurs:

   ```
    ERROR 1105 (HY000): errCode = 2, detailMessage = [INTERNAL_ERROR]UdfRuntimeException: JDBC executor sql has error:
    CAUSED BY: CommunicationsException: Communications link failure
    The last packet successfully received from the server was 4,446 milliseconds ago. The last packet sent successfully to the server was 4,446 milliseconds ago.
    ```

    When the above phenomenon appears, it may be that mysql server's own memory or CPU resources are exhausted and the MySQL service is unavailable. You can try to increase the memory or CPU resources of MySQL Server.
