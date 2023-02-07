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

1. Supported datas sources include MySQL, PostgreSQL, Oracle, SQLServer, and Clickhouse.

## Create Catalog

<version since="1.2.0">

1. MySQL

</version>

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

<version since="1.2.2">

2. PostgreSQL

</version>

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

   As for data mapping from PostgreSQL to Doris, one Database in Doris corresponds to one schema in the specified database in PostgreSQL (for example, "demo" in `jdbc_url`  above), and one Table in that Database corresponds to one table in that schema. To make it more intuitive, the mapping relations are as follows:

   | Doris    | PostgreSQL |
   | -------- | ---------- |
   | Catalog  | Database   |
   | Database | Schema     |
   | Table    | Table      |

<version since="1.2.2">

3. Oracle

</version>

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

   As for data mapping from Oracle to Doris, one Database in Doris corresponds to one User (for example, "helowin" in `jdbc_url`  above), and one Table in that Database corresponds to one table that the User has access to. In conclusion, the mapping relations are as follows:

   | Doris    | PostgreSQL |
   | -------- | ---------- |
   | Catalog  | Database   |
   | Database | User       |
   | Table    | Table      |

<version since="1.2.2">

4. Clickhouse

</version>

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

<version since="1.2.2">

5. SQLServer

</version>

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

### Parameter Description

| Parameter       | Required or Not | Default Value | Description                                        |
| --------------- | --------------- | ------------- | -------------------------------------------------- |
| `user`          | Yes             |               | Username in relation to the corresponding database |
| `password`      | Yes             |               | Password for the corresponding database            |
| `jdbc_url `     | Yes             |               | JDBC connection string                             |
| `driver_url `   | Yes             |               | JDBC Driver Jar                                    |
| `driver_class ` | Yes             |               | JDBC Driver Class                                  |

> `driver_url` can be specified in three ways:
>
> 1. File name. For example,  `mysql-connector-java-5.1.47.jar`. Please place the Jar file package in  `jdbc_drivers/`  under the FE/BE deployment directory in advance so the system can locate the file. You can change the location of the file by modifying  `jdbc_drivers_dir`  in fe.conf and be.conf.
>
> 2. Local absolute path. For example, `file:///path/to/mysql-connector-java-5.1.47.jar`. Please place the Jar file package in the specified paths of FE/BE node.
>
> 3. HTTP address. For example, `https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com/jdbc_driver/mysql-connector-java-5.1.47.jar`. The system will download the Driver file from the HTTP address. This only supports HTTP services with no authentication requirements.

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
| UNSIGNED BIGINT                                              | STRING      |                                                              |
| FLOAT                                                        | FLOAT       |                                                              |
| DOUBLE                                                       | DOUBLE      |                                                              |
| DECIMAL                                                      | DECIMAL     |                                                              |
| DATE                                                         | DATE        |                                                              |
| TIMESTAMP                                                    | DATETIME    |                                                              |
| DATETIME                                                     | DATETIME    |                                                              |
| YEAR                                                         | SMALLINT    |                                                              |
| TIME                                                         | STRING      |                                                              |
| CHAR                                                         | CHAR        |                                                              |
| VARCHAR                                                      | STRING      |                                                              |
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
| number(p) / number(p,0)       |             | Doris will determine the type to map to based on the value of p: `p < 3` -> `TINYINT`; `p < 5` -> `SMALLINT`; `p < 10` -> `INT`; `p < 19` -> `BIGINT`; `p > 19` -> `LARGEINT` |
| number(p,s)                   | DECIMAL     |                                                              |
| decimal                       | DECIMAL     |                                                              |
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

| ClickHouse Type        | Doris Type  | Comment                                                      |
| ---------------------- | ----------- | ------------------------------------------------------------ |
| Bool                   | BOOLEAN     |                                                              |
| String                 | STRING      |                                                              |
| Date/Date32            | DATE        |                                                              |
| DateTime/DateTime64    | DATETIME    | Data beyond the maximum precision of DateTime in Doris will be truncated. |
| Float32                | FLOAT       |                                                              |
| Float64                | DOUBLE      |                                                              |
| Int8                   | TINYINT     |                                                              |
| Int16/UInt8            | SMALLINT    | Doris does not support UNSIGNED data types so UInt8 will be mapped to SMALLINT. |
| Int32/UInt16           | INT         | Doris does not support UNSIGNED data types so UInt16 will be mapped to INT. |
| Int64/Uint32           | BIGINT      | Doris does not support UNSIGNED data types so UInt32 will be mapped to BIGINT. |
| Int128/UInt64          | LARGEINT    | Doris does not support UNSIGNED data types so UInt64 will be mapped to LARGEINT. |
| Int256/UInt128/UInt256 | STRING      | Doris does not support data types of such orders of magnitude so these will be mapped to STRING. |
| DECIMAL                | DECIMAL     | Data beyond the maximum decimal precision in Doris will be truncated. |
| Enum/IPv4/IPv6/UUID    | STRING      | Data of IPv4 and IPv6 type will be displayed with an extra `/` as a prefix. To remove the `/`, you can use the `split_part`function. |
| Other                  | UNSUPPORTED |                                                              |

## FAQ

1. Are there any other databases supported besides MySQL, Oracle, PostgreSQL, SQLServer, and ClickHouse?

   Currently, Doris supports MySQL, Oracle, PostgreSQL, SQLServer, and ClickHouse. We are planning to expand this list. Technically, any databases that support JDBC access can be connected to Doris in the form of JDBC external tables. You are more than welcome to be a Doris contributor to expedite this effort.

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
