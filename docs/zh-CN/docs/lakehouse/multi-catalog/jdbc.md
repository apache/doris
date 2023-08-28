---
{
    "title": "JDBC",
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


# JDBC

JDBC Catalog 通过标准 JDBC 协议，连接其他数据源。

连接后，Doris 会自动同步数据源下的 Database 和 Table 的元数据，以便快速访问这些外部数据。

## 使用限制

支持 MySQL、PostgreSQL、Oracle、SQLServer、Clickhouse、Doris、SAP HANA、Trino/Presto、OceanBase

## 语法
    
```sql
CREATE CATALOG <catalog_name>
PROPERTIES ("key"="value", ...)
```

## 参数说明

| 参数                      | 必须 | 默认值  | 说明                                                                                          |
|---------------------------|-----|---------|---------------------------------------------------------------------------------------------|
| `user`                    | 是   |         | 对应数据库的用户名                                                                            |
| `password`                | 是   |         | 对应数据库的密码                                                                              |
| `jdbc_url`                | 是   |         | JDBC 连接串                                                                                   |
| `driver_url`              | 是   |         | JDBC Driver Jar 包名称                                                                        |
| `driver_class`            | 是   |         | JDBC Driver Class 名称                                                                        |
| `lower_case_table_names`  | 否   | "false" | 是否以小写的形式同步jdbc外部数据源的库名和表名                                                |
| `only_specified_database` | 否   | "false" | 指定是否只同步指定的 database                                                                 |
| `include_database_list`   | 否   | ""      | 当only_specified_database=true时，指定同步多个database，以','分隔。db名称是大小写敏感的。         |
| `exclude_database_list`   | 否   | ""      | 当only_specified_database=true时，指定不需要同步的多个database，以','分割。db名称是大小写敏感的。 |

:::tip
`driver_url` 可以通过以下三种方式指定：

1. 文件名。如 `mysql-connector-java-5.1.47.jar`。需将 Jar 包预先存放在 FE 和 BE 部署目录的 `jdbc_drivers/` 目录下。系统会自动在这个目录下寻找。该目录的位置，也可以由 fe.conf 和 be.conf 中的 `jdbc_drivers_dir` 配置修改。

2. 本地绝对路径。如 `file:///path/to/mysql-connector-java-5.1.47.jar`。需将 Jar 包预先存放在所有 FE/BE 节点指定的路径下。

3. Http 地址。如：`https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com/jdbc_driver/mysql-connector-java-8.0.25.jar`。系统会从这个 http 地址下载 Driver 文件。仅支持无认证的 http 服务。
:::

:::tip
`only_specified_database`:
在jdbc连接时可以指定链接到哪个database/schema, 如：mysql中jdbc_url中可以指定database, pg的jdbc_url中可以指定currentSchema。

`include_database_list`:
仅在`only_specified_database=true`时生效，指定需要同步的 database，以','分割，db名称是大小写敏感的。

`exclude_database_list`:
仅在`only_specified_database=true`时生效，指定不需要同步的多个database，以','分割，db名称是大小写敏感的。

当 `include_database_list` 和 `exclude_database_list` 有重合的database配置时，`exclude_database_list`会优先生效。

如果使用该参数时连接oracle数据库，要求使用ojdbc8.jar以上版本jar包。
:::

## 数据查询

### 示例

```sql
select * from mysql_catalog.mysql_database.mysql_table where k1 > 1000 and k3 ='term';
```
:::tip
由于可能存在使用数据库内部的关键字作为字段名，为解决这种状况下仍能正确查询，所以在 SQL 语句中，会根据各个数据库的标准自动在字段名与表名上加上转义符。例如 MYSQL(``)、PostgreSQL("")、SQLServer([])、ORACLE("")，所以此时可能会造成字段名的大小写敏感，具体可以通过explain sql，查看转义后下发到各个数据库的查询语句。
:::

### 谓词下推

1. 当执行类似于 `where dt = '2022-01-01'` 这样的查询时，Doris 能够将这些过滤条件下推到外部数据源，从而直接在数据源层面排除不符合条件的数据，减少了不必要的数据获取和传输。这大大提高了查询性能，同时也降低了对外部数据源的负载。
   
2. 当 `enable_func_pushdown` 设置为true，会将 where 之后的函数条件也下推到外部数据源，目前仅支持 MySQL，如遇到 MySQL 不支持的函数，可以将此参数设置为 false，目前 Doris 会自动识别部分 MySQL 不支持的函数进行下推条件过滤，可通过 explain sql 查看。

目前不会下推的函数有：

|    MYSQL     |
|:------------:|
|  DATE_TRUNC  |
| MONEY_FORMAT |

### 行数限制

如果在查询中带有 limit 关键字，Doris 会将其转译成适合不同数据源的语义。

## 数据写入

在 Doris 中建立 JDBC Catalog 后，可以通过 insert into 语句直接写入数据，也可以将 Doris 执行完查询之后的结果写入 JDBC Catalog，或者是从一个 JDBC Catalog 将数据导入另一个 JDBC Catalog。

### 示例

```sql
insert into mysql_catalog.mysql_database.mysql_table values(1, "doris");
insert into mysql_catalog.mysql_database.mysql_table select * from table;
```

### 事务

Doris 的数据是由一组 batch 的方式写入 JDBC Catalog 的，如果中途导入中断，之前写入数据可能需要回滚。所以 JDBC Catalog 支持数据写入时的事务，事务的支持需要通过设置 session variable: `enable_odbc_transcation `。

```sql
set enable_odbc_transcation = true; 
```

事务保证了JDBC外表数据写入的原子性，但是一定程度上会降低数据写入的性能，可以考虑酌情开启该功能。

## 使用指南

### MySQL

#### 创建示例

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

#### 层级映射

|  Doris   |    MySQL     |
|:--------:|:------------:|
| Catalog  | MySQL Server |
| Database |   Database   |
|  Table   |    Table     |

#### 类型映射

| MYSQL Type                                | Doris Type     | Comment                                         |
|-------------------------------------------|----------------|-------------------------------------------------|
| BOOLEAN                                   | TINYINT        |                                                 |
| TINYINT                                   | TINYINT        |                                                 |
| SMALLINT                                  | SMALLINT       |                                                 |
| MEDIUMINT                                 | INT            |                                                 |
| INT                                       | INT            |                                                 |
| BIGINT                                    | BIGINT         |                                                 |
| UNSIGNED TINYINT                          | SMALLINT       | Doris 没有 UNSIGNED 数据类型，所以扩大一个数量级 |
| UNSIGNED MEDIUMINT                        | INT            | Doris 没有 UNSIGNED 数据类型，所以扩大一个数量级 |
| UNSIGNED INT                              | BIGINT         | Doris 没有 UNSIGNED 数据类型，所以扩大一个数量级 |
| UNSIGNED BIGINT                           | LARGEINT       |                                                 |
| FLOAT                                     | FLOAT          |                                                 |
| DOUBLE                                    | DOUBLE         |                                                 |
| DECIMAL                                   | DECIMAL        |                                                 |
| DATE                                      | DATE           |                                                 |
| TIMESTAMP                                 | DATETIME       |                                                 |
| DATETIME                                  | DATETIME       |                                                 |
| YEAR                                      | SMALLINT       |                                                 |
| TIME                                      | STRING         |                                                 |
| CHAR                                      | CHAR           |                                                 |
| VARCHAR                                   | VARCHAR        |                                                 |
| JSON                                      | JSON           |                                                 |
| SET                                       | STRING         |                                                 |
| BIT                                       | BOOLEAN/STRING | BIT(1) 会映射为 BOOLEAN,其他 BIT 映射为 STRING  |
| TINYTEXT、TEXT、MEDIUMTEXT、LONGTEXT         | STRING         |                                                 |
| BLOB、MEDIUMBLOB、LONGBLOB、TINYBLOB         | STRING         |                                                 |
| TINYSTRING、STRING、MEDIUMSTRING、LONGSTRING | STRING         |                                                 |
| BINARY、VARBINARY                          | STRING         |                                                 |
| Other                                     | UNSUPPORTED    |                                                 |

### PostgreSQL

#### 创建示例

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

#### 层级映射

映射 PostgreSQL 时，Doris 的一个 Database 对应于 PostgreSQL 中指定Catalog下的一个 Schema（如示例中 `jdbc_url` 参数中 "demo"下的schemas）。而 Doris 的 Database 下的 Table 则对应于 PostgreSQL 中，Schema 下的 Tables。即映射关系如下：

|  Doris   | PostgreSQL |
|:--------:|:----------:|
| Catalog  |  Database  |
| Database |   Schema   |
|  Table   |   Table    |

:::tip
Doris 通过sql 语句 `select nspname from pg_namespace where has_schema_privilege('<UserName>', nspname, 'USAGE');` 来获得 PG user 能够访问的所有 schema 并将其映射为 Doris 的 database
:::

#### 类型映射

 | POSTGRESQL Type                         | Doris Type     | Comment                                       |
 |-----------------------------------------|----------------|-----------------------------------------------|
 | boolean                                 | BOOLEAN        |                                               |
 | smallint/int2                           | SMALLINT       |                                               |
 | integer/int4                            | INT            |                                               |
 | bigint/int8                             | BIGINT         |                                               |
 | decimal/numeric                         | DECIMAL        |                                               |
 | real/float4                             | FLOAT          |                                               |
 | double precision                        | DOUBLE         |                                               |
 | smallserial                             | SMALLINT       |                                               |
 | serial                                  | INT            |                                               |
 | bigserial                               | BIGINT         |                                               |
 | char                                    | CHAR           |                                               |
 | varchar/text                            | STRING         |                                               |
 | timestamp                               | DATETIME       |                                               |
 | date                                    | DATE           |                                               |
 | json/josnb                              | JSON           |                                               |
 | time                                    | STRING         |                                               |
 | interval                                | STRING         |                                               |
 | point/line/lseg/box/path/polygon/circle | STRING         |                                               |
 | cidr/inet/macaddr                       | STRING         |                                               |
 | bit                                     | BOOLEAN/STRING | bit(1)会映射为 BOOLEAN,其他 bit 映射为 STRING |
 | uuid                                    | STRING         |                                               |
 | Other                                   | UNSUPPORTED    |                                               |

### Oracle

#### 创建示例

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

#### 层级映射

映射 Oracle 时，Doris 的一个 Database 对应于 Oracle 中的一个 User。而 Doris 的 Database 下的 Table 则对应于 Oracle 中，该 User 下的有权限访问的 Table。即映射关系如下：

|  Doris   |  Oracle  |
|:--------:|:--------:|
| Catalog  | Database |
| Database |   User   |
|  Table   |  Table   |

**注意：** 当前不支持同步 Oracle 的 SYNONYM TABLE

#### 类型映射

| ORACLE Type                       | Doris Type                           | Comment                                                                                                                                         |
|-----------------------------------|--------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------|
| number(p) / number(p,0)           | TINYINT/SMALLINT/INT/BIGINT/LARGEINT | Doris会根据p的大小来选择对应的类型：`p < 3` -> `TINYINT`; `p < 5` -> `SMALLINT`; `p < 10` -> `INT`; `p < 19` -> `BIGINT`; `p > 19` -> `LARGEINT` |
| number(p,s), [ if(s>0 && p>s) ]   | DECIMAL(p,s)                         |                                                                                                                                                 |
| number(p,s), [ if(s>0 && p < s) ] | DECIMAL(s,s)                         |                                                                                                                                                 |
| number(p,s), [ if(s<0) ]          | TINYINT/SMALLINT/INT/BIGINT/LARGEINT | s<0的情况下, Doris会将p设置为 p+\|s\|, 并进行和number(p) / number(p,0)一样的映射                                                                |
| number                            |                                      | Doris目前不支持未指定p和s的oracle类型                                                                                                           |
| decimal                           | DECIMAL                              |                                                                                                                                                 |
| float/real                        | DOUBLE                               |                                                                                                                                                 |
| DATE                              | DATETIME                             |                                                                                                                                                 |
| TIMESTAMP                         | DATETIME                             |                                                                                                                                                 |
| CHAR/NCHAR                        | STRING                               |                                                                                                                                                 |
| VARCHAR2/NVARCHAR2                | STRING                               |                                                                                                                                                 |
| LONG/ RAW/ LONG RAW/ INTERVAL     | STRING                               |                                                                                                                                                 |
| Other                             | UNSUPPORTED                          |                                                                                                                                                 |

### SQLServer

#### 创建示例

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

#### 层级映射

映射 SQLServer 时，Doris 的一个 Database 对应于 SQLServer 中指定 Database（如示例中 `jdbc_url` 参数中的 "doris_test"）下的一个 Schema。而 Doris 的 Database 下的 Table 则对应于 SQLServer 中，Schema 下的 Tables。即映射关系如下：

|  Doris   | SQLServer |
|:--------:|:---------:|
| Catalog  | Database  |
| Database |  Schema   |
|  Table   |   Table   |

#### 类型映射

| SQLServer Type                         | Doris Type    | Comment                                                      |
|----------------------------------------|---------------|--------------------------------------------------------------|
| bit                                    | BOOLEAN       |                                                              |
| tinyint                                | SMALLINT      | SQLServer 的 tinyint 是无符号数，所以映射为 Doris 的 SMALLINT |
| smallint                               | SMALLINT      |                                                              |
| int                                    | INT           |                                                              |
| bigint                                 | BIGINT        |                                                              |
| real                                   | FLOAT         |                                                              |
| float                                  | DOUBLE        |                                                              |
| money                                  | DECIMAL(19,4) |                                                              |
| smallmoney                             | DECIMAL(10,4) |                                                              |
| decimal/numeric                        | DECIMAL       |                                                              |
| date                                   | DATE          |                                                              |
| datetime/datetime2/smalldatetime       | DATETIMEV2    |                                                              |
| char/varchar/text/nchar/nvarchar/ntext | STRING        |                                                              |
| binary/varbinary                       | STRING        |                                                              |
| time/datetimeoffset                    | STRING        |                                                              |
| Other                                  | UNSUPPORTED   |                                                              |

### Doris

Jdbc Catalog也支持连接另一个Doris数据库：

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

#### 类型映射

| Doris Type | Jdbc Catlog Doris Type | Comment                                              |
|------------|------------------------|------------------------------------------------------|
| BOOLEAN    | BOOLEAN                |                                                      |
| TINYINT    | TINYINT                |                                                      |
| SMALLINT   | SMALLINT               |                                                      |
| INT        | INT                    |                                                      |
| BIGINT     | BIGINT                 |                                                      |
| LARGEINT   | LARGEINT               |                                                      |
| FLOAT      | FLOAT                  |                                                      |
| DOUBLE     | DOUBLE                 |                                                      |
| DECIMALV3  | DECIMALV3/STRING       | 将根据 DECIMAL 字段的（precision, scale）选择用何种类型 |
| DATE       | DATE                   |                                                      |
| DATETIME   | DATETIME               |                                                      |
| CHAR       | CHAR                   |                                                      |
| VARCHAR    | VARCHAR                |                                                      |
| STRING     | STRING                 |                                                      |
| TEXT       | STRING                 |                                                      |
| HLL        | HLL                    | 查询HLL需要设置`return_object_data_as_binary=true`   |
| Array      | Array                  | Array内部类型适配逻辑参考上述类型，不支持嵌套复杂类型        |
| Other      | UNSUPPORTED            |                                                      |

### Clickhouse

#### 创建示例

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

#### 层级映射

|  Doris   |    ClickHouse     |
|:--------:|:-----------------:|
| Catalog  | ClickHouse Server |
| Database |     Database      |
|  Table   |       Table       |

#### 类型映射

| ClickHouse Type        | Doris Type       | Comment                                                |
|------------------------|------------------|--------------------------------------------------------|
| Bool                   | BOOLEAN          |                                                        |
| String                 | STRING           |                                                        |
| Date/Date32            | DATE             |                                                        |
| DateTime/DateTime64    | DATETIME         |                                                        |
| Float32                | FLOAT            |                                                        |
| Float64                | DOUBLE           |                                                        |
| Int8                   | TINYINT          |                                                        |
| Int16/UInt8            | SMALLINT         | Doris 没有 UNSIGNED 数据类型，所以扩大一个数量级        |
| Int32/UInt16           | INT              | Doris 没有 UNSIGNED 数据类型，所以扩大一个数量级        |
| Int64/Uint32           | BIGINT           | Doris 没有 UNSIGNED 数据类型，所以扩大一个数量级        |
| Int128/UInt64          | LARGEINT         | Doris 没有 UNSIGNED 数据类型，所以扩大一个数量级        |
| Int256/UInt128/UInt256 | STRING           | Doris 没有这个数量级的数据类型，采用 STRING 处理        |
| DECIMAL                | DECIMALV3/STRING | 将根据 DECIMAL 字段的（precision, scale) 选择用何种类型 |
| Enum/IPv4/IPv6/UUID    | STRING           |                                                        |
| Array                  | ARRAY            | Array内部类型适配逻辑参考上述类型，不支持嵌套类型       |
| Other                  | UNSUPPORTED      |                                                        |


### SAP HANA

#### 创建示例

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

#### 层级映射

|  Doris   | SAP HANA |
|:--------:|:--------:|
| Catalog  | Database |
| Database |  Schema  |
|  Table   |  Table   |

#### 类型映射

| SAP HANA Type | Doris Type       | Comment                                                   |
|---------------|------------------|-----------------------------------------------------------|
| BOOLEAN       | BOOLEAN          |                                                           |
| TINYINT       | TINYINT          |                                                           |
| SMALLINT      | SMALLINT         |                                                           |
| INTERGER      | INT              |                                                           |
| BIGINT        | BIGINT           |                                                           |
| SMALLDECIMAL  | DECIMALV3        |                                                           |
| DECIMAL       | DECIMALV3/STRING | 将根据Doris DECIMAL字段的（precision, scale）选择用何种类型 |
| REAL          | FLOAT            |                                                           |
| DOUBLE        | DOUBLE           |                                                           |
| DATE          | DATE             |                                                           |
| TIME          | STRING           |                                                           |
| TIMESTAMP     | DATETIME         |                                                           |
| SECONDDATE    | DATETIME         |                                                           |
| VARCHAR       | STRING           |                                                           |
| NVARCHAR      | STRING           |                                                           |
| ALPHANUM      | STRING           |                                                           |
| SHORTTEXT     | STRING           |                                                           |
| CHAR          | CHAR             |                                                           |
| NCHAR         | CHAR             |                                                           |


### Trino/Presto

#### 创建示例

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

#### 层级映射

映射 Trino 时，Doris 的 Database 对应于 Trino 中指定 Catalog（如示例中 `jdbc_url` 参数中的 "hive"）下的一个 Schema。而 Doris 的 Database 下的 Table 则对应于 Trino 中 Schema 下的 Tables。即映射关系如下：

|  Doris   | Trino/Presto |
|:--------:|:------------:|
| Catalog  |   Catalog    |
| Database |    Schema    |
|  Table   |    Table     |


#### 类型映射

| Trino/Presto Type | Doris Type               | Comment                                               |
|-------------------|--------------------------|-------------------------------------------------------|
| boolean           | BOOLEAN                  |                                                       |
| tinyint           | TINYINT                  |                                                       |
| smallint          | SMALLINT                 |                                                       |
| integer           | INT                      |                                                       |
| bigint            | BIGINT                   |                                                       |
| decimal           | DECIMAL/DECIMALV3/STRING | 将根据 DECIMAL 字段的（precision, scale）选择用何种类型 |
| real              | FLOAT                    |                                                       |
| double            | DOUBLE                   |                                                       |
| date              | DATE                     |                                                       |
| timestamp         | DATETIME                 |                                                       |
| varchar           | TEXT                     |                                                       |
| char              | CHAR                     |                                                       |
| array             | ARRAY                    | Array 内部类型适配逻辑参考上述类型，不支持嵌套类型     |
| others            | UNSUPPORTED              |                                                       |


### OceanBase

#### 创建示例

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
 Doris 在连接 OceanBase 时，会自动识别 OceanBase 处于 MySQL 或者 Oracle 模式，层级对应和类型映射参考 [MySQL](#MySQL) 与 [Oracle](#Oracle)
:::

### 查看 JDBC Catalog

可以通过 SHOW CATALOGS 查询当前所在 Doris 集群里所有 Catalog：

```sql
SHOW CATALOGS;
```

通过 SHOW CREATE CATALOG 查询某个 Catalog 的创建语句：

```sql
SHOW CREATE CATALOG <catalog_name>;
```

### 删除 JDBC Catalog

可以通过 DROP CATALOG 删除某个 Catalog：

```sql
DROP CATALOG <catalog_name>;
```

### 查询 JDBC Catalog

1. 通过 SWITCH 切换当前会话生效的 Catalog：

    ```sql
    SWITCH <catalog_name>;
    ```

2. 通过 SHOW DATABASES 查询当前 Catalog 下的所有库：

    ```sql
    SHOW DATABASES FROM <catalog_name>;
    ```

    ```sql
    SHOW DATABASES;
    ```

3. 通过 USE 切换当前会话生效的 Database：

    ```sql
    USE <database_name>;
    ```

    或者直接通过 `USE <catalog_name>.<database_name>;` 切换当前会话生效的 Database

4. 通过 SHOW TABLES 查询当前 Catalog 下的所有表：

    ```sql
    SHOW TABLES FROM <catalog_name>.<database_name>;
    ```

    ```sql
    SHOW TABLES FROM <database_name>;
    ```

    ```sql
    SHOW TABLES;
    ```

5. 通过 SELECT 查询当前 Catalog 下的某个表的数据：

    ```sql
    SELECT * FROM <table_name>;
    ```

## 常见问题

1. 除了 MySQL,Oracle,PostgreSQL,SQLServer,ClickHouse,SAP HANA,Trino/Presto,OceanBase 是否能够支持更多的数据库

    目前Doris只适配了 MySQL,Oracle,PostgreSQL,SQLServer,ClickHouse,SAP HANA,Trino/Presto,OceanBase. 关于其他的数据库的适配工作正在规划之中，原则上来说任何支持JDBC访问的数据库都能通过JDBC外表来访问。如果您有访问其他外表的需求，欢迎修改代码并贡献给Doris。

2. 读写 MySQL外表的emoji表情出现乱码

    Doris进行jdbc外表连接时，由于mysql之中默认的utf8编码为utf8mb3，无法表示需要4字节编码的emoji表情。这里需要在建立mysql外表时设置对应列的编码为utf8mb4,设置服务器编码为utf8mb4,JDBC Url中的characterEncoding不配置.（该属性不支持utf8mb4,配置了非utf8mb4将导致无法写入表情，因此要留空，不配置）

    可全局修改配置项
    
    ```
    修改mysql目录下的my.ini文件（linux系统为etc目录下的my.cnf文件）
    [client]
    default-character-set=utf8mb4
    
    [mysql]
    设置mysql默认字符集
    default-character-set=utf8mb4
    
    [mysqld]
    设置mysql字符集服务器
    character-set-server=utf8mb4
    collation-server=utf8mb4_unicode_ci
    init_connect='SET NAMES utf8mb4
    
    修改对应表与列的类型
    ALTER TABLE table_name MODIFY  colum_name  VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    ALTER TABLE table_name CHARSET=utf8mb4;
    SET NAMES utf8mb4
    ```

3. 读 MySQL 外表时，DateTime="0000:00:00 00:00:00"异常报错: "CAUSED BY: DataReadException: Zero date value prohibited"

    这是因为JDBC中对于该非法的DateTime默认处理为抛出异常，可以通过参数 `zeroDateTimeBehavior`控制该行为。
    
    可选参数为: `EXCEPTION`,`CONVERT_TO_NULL`,`ROUND`, 分别为：异常报错，转为NULL值，转为 "0001-01-01 00:00:00";
    
    可在url中添加: `"jdbc_url"="jdbc:mysql://IP:PORT/doris_test?zeroDateTimeBehavior=convertToNull"`

4. 读取 MySQL 外表或其他外表时，出现加载类失败

    如以下异常：
 
    ```
    failed to load driver class com.mysql.jdbc.driver in either of hikariconfig class loader
    ```
 
    这是因为在创建 catalog 时，填写的driver_class不正确，需要正确填写，如上方例子为大小写问题，应填写为 `"driver_class" = "com.mysql.jdbc.Driver"`

5. 读取 MySQL 出现通信链路异常

    如果出现如下报错：

    ```
    ERROR 1105 (HY000): errCode = 2, detailMessage = PoolInitializationException: Failed to initialize pool: Communications link failure
    
    The last packet successfully received from the server was 7 milliseconds ago.  The last packet sent successfully to the server was 4 milliseconds ago.
    CAUSED BY: CommunicationsException: Communications link failure
        
    The last packet successfully received from the server was 7 milliseconds ago.  The last packet sent successfully to the server was 4 milliseconds ago.
    CAUSED BY: SSLHandshakeExcepti
    ```
    
    可查看be的be.out日志
    
    如果包含以下信息：
    
    ```
    WARN: Establishing SSL connection without server's identity verification is not recommended. 
    According to MySQL 5.5.45+, 5.6.26+ and 5.7.6+ requirements SSL connection must be established by default if explicit option isn't set. 
    For compliance with existing applications not using SSL the verifyServerCertificate property is set to 'false'. 
    You need either to explicitly disable SSL by setting useSSL=false, or set useSSL=true and provide truststore for server certificate verification.
    ```

    可在创建 Catalog 的 `jdbc_url` 把JDBC连接串最后增加 `?useSSL=false` ,如 `"jdbc_url" = "jdbc:mysql://127.0.0.1:3306/test?useSSL=false"`

6. 查询MYSQL的数据库报OutOfMemoryError的错误

    为减少内存的使用，在获取结果集时，每次仅获取batchSize的大小，这样一批一批的获取结果。而MYSQL默认是一次将结果全部加载到内存，
    设置的按批获取无法生效，需要主动显示的在URL中指定:"jdbc_url"="jdbc:mysql://IP:PORT/doris_test?useCursorFetch=true"

7. 在使用JDBC查询过程中时，如果出现"CAUSED BY: SQLException OutOfMemoryError" 类似的错误

    如果MYSQL已经主动设置useCursorFetch，可以在be.conf中修改jvm_max_heap_size的值，尝试增大JVM的内存，目前默认值为1024M。

8. 使用JDBC查询MYSQL大数据量时，如果查询偶尔能够成功，偶尔会报如下错误，且出现该错误时MYSQL的连接被全部断开，无法连接到MYSQL SERVER，过段时间后mysql又恢复正常，但是之前的连接都没了：

    ```
    ERROR 1105 (HY000): errCode = 2, detailMessage = [INTERNAL_ERROR]UdfRuntimeException: JDBC executor sql has error:
    CAUSED BY: CommunicationsException: Communications link failure
    The last packet successfully received from the server was 4,446 milliseconds ago. The last packet sent successfully to the server was 4,446 milliseconds ago.
    ```

    出现上述现象时，可能是Mysql Server自身的内存或CPU资源被耗尽导致Mysql服务不可用，可以尝试增大Mysql Server的内存或CPU配置。
 
9. 使用JDBC查询MYSQL的过程中，如果发现和在MYSQL库的查询结果不一致的情况

    首先要先排查下查询字段中是字符串否存在有大小写情况。比如，Table中有一个字段c_1中有"aaa"和"AAA"两条数据，如果在初始化MYSQL数据库时未指定区分字符串
    大小写，那么MYSQL默认是不区分字符串大小写的，但是在Doris中是严格区分大小写的，所以会出现以下情况：

    ```
    Mysql行为：
    select count(c_1) from table where c_1 = "aaa"; 未区分字符串大小，所以结果为：2

    Doris行为：
    select count(c_1) from table where c_1 = "aaa"; 严格区分字符串大小，所以结果为：1
    ```

    如果出现上述现象，那么需要按照需求来调整，方式如下：
    
    在MYSQL中查询时添加“BINARY”关键字来强制区分大小写：select count(c_1) from table where BINARY c_1 = "aaa"; 或者在MYSQL中建表时候指定：
    CREATE TABLE table ( c_1 VARCHAR(255) CHARACTER SET binary ); 或者在初始化MYSQL数据库时指定校对规则来区分大小写：
    character-set-server=UTF-8 和 collation-server=utf8_bin。

10. 读取 SQLServer 出现通信链路异常

    ```
    ERROR 1105 (HY000): errCode = 2, detailMessage = (10.16.10.6)[CANCELLED][INTERNAL_ERROR]UdfRuntimeException: Initialize datasource failed:
    CAUSED BY: SQLServerException: The driver could not establish a secure connection to SQL Server by using Secure Sockets Layer (SSL) encryption.
    Error: "sun.security.validator.ValidatorException: PKIX path building failed: sun.security.provider.certpath.SunCertPathBuilderException:
    unable to find valid certification path to requested target". ClientConnectionId:a92f3817-e8e6-4311-bc21-7c66
    ```

    可在创建 Catalog 的 `jdbc_url` 把JDBC连接串最后增加 `encrypt=false` ,如 `"jdbc_url" = "jdbc:sqlserver://127.0.0.1:1433;DataBaseName=doris_test;encrypt=false"`

11. 读取 MySQL datetime 类型出现异常

    ```
    ERROR 1105 (HY000): errCode = 2, detailMessage = (10.16.10.6)[INTERNAL_ERROR]UdfRuntimeException: get next block failed: 
    CAUSED BY: SQLException: Zero date value prohibited
    CAUSED BY: DataReadException: Zero date value prohibited
    ```
    
    这是因为 JDBC 并不能处理 0000-00-00 00:00:00 这种时间格式，
    需要在创建 Catalog 的 `jdbc_url` 把JDBC连接串最后增加 `zeroDateTimeBehavior=convertToNull` ,如 `"jdbc_url" = "jdbc:mysql://127.0.0.1:3306/test?zeroDateTimeBehavior=convertToNull"`
    这种情况下，JDBC 会把 0000-00-00 00:00:00 转换成 null，然后 Doris 会把 DateTime 类型的列按照可空类型处理，这样就可以正常读取了。

12. 读取 Oracle 出现 `Non supported character set (add orai18n.jar in your classpath): ZHS16GBK` 异常
    
    下载 [orai18n.jar](https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html) 并放到 Doris FE 的 lib 目录以及 BE 的 lib/java_extensions 目录 (Doris 2.0 之前的版本需放到 BE 的 lib 目录下) 下即可。

13. 通过jdbc catalog 读取Clickhouse数据出现`NoClassDefFoundError: net/jpountz/lz4/LZ4Factory` 错误信息
    
    可以先下载[lz4-1.3.0.jar](https://repo1.maven.org/maven2/net/jpountz/lz4/lz4/1.3.0/lz4-1.3.0.jar)包，然后放到DorisFE lib 目录以及BE 的 `lib/lib/java_extensions`目录中（Doris 2.0 之前的版本需放到 BE 的 lib 目录下）。
