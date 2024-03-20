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

| 参数                        | 必须 | 默认值     | 说明                                                                    |
|---------------------------|-----|---------|-----------------------------------------------------------------------|
| `user`                    | 是   |         | 对应数据库的用户名                                                             |
| `password`                | 是   |         | 对应数据库的密码                                                              |
| `jdbc_url`                | 是   |         | JDBC 连接串                                                              |
| `driver_url`              | 是   |         | JDBC Driver Jar 包名称                                                   |
| `driver_class`            | 是   |         | JDBC Driver Class 名称                                                  |
| `lower_case_meta_names`   | 否   | "false" | 是否以小写的形式同步jdbc外部数据源的库名和表名以及列名                                         |
| `meta_names_mapping`    | 否   | ""      | 当jdbc外部数据源存在名称相同只有大小写不同的情况，例如 DORIS 和 doris，Doris 由于歧义而在查询 Catalog 时报错，此时需要配置 `meta_names_mapping` 参数来解决冲突。 |
| `only_specified_database` | 否   | "false" | 指定是否只同步指定的 database                                                   |
| `include_database_list`   | 否   | ""      | 当only_specified_database=true时，指定同步多个database，以','分隔。db名称是大小写敏感的。     |
| `exclude_database_list`   | 否   | ""      | 当only_specified_database=true时，指定不需要同步的多个database，以','分割。db名称是大小写敏感的。 |
| `test_connection`         | 否   | "true" | 是否在创建 Catalog 时测试连接。如果设置为 `true`，则会在创建 Catalog 时测试连接，如果连接失败，则会拒绝创建 Catalog。如果设置为 `false`，则不会测试连接。 |

### 驱动包路径

`driver_url` 可以通过以下三种方式指定：

1. 文件名。如 `mysql-connector-java-8.0.25.jar`。需将 Jar 包预先存放在 FE 和 BE 部署目录的 `jdbc_drivers/` 目录下。系统会自动在这个目录下寻找。该目录的位置，也可以由 fe.conf 和 be.conf 中的 `jdbc_drivers_dir` 配置修改。

2. 本地绝对路径。如 `file:///path/to/mysql-connector-java-8.0.25.jar`。需将 Jar 包预先存放在所有 FE/BE 节点指定的路径下。

3. Http 地址。如：`https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com/jdbc_driver/mysql-connector-java-8.0.25.jar`。系统会从这个 http 地址下载 Driver 文件。仅支持无认证的 http 服务。

**驱动包安全性**

为了防止在创建 Catalog 时使用了未允许路径的 Driver Jar 包，Doris 会对 Jar 包进行路径管理和校验和检查。

1. 针对上述方式 1，Doris 默认用户配置的 `jdbc_drivers_dir` 和其目录下的所有 Jar 包都是安全的，不会对其进行路径检查。

2. 针对上述方式 2、3 ，Doris 会对 Jar 包的来源进行检查，检查规则如下： 
   
   * 通过 FE 配置项 `jdbc_driver_secure_path` 来控制允许的驱动包路径，该配置项可配置多个路径，以分号分隔。当配置了该项时，Doris 会检查 Catalog properties 中 driver_url 的路径是的部分前缀是否在 `jdbc_driver_secure_path` 中，如果不在其中，则会拒绝创建 Catalog。
   * 此参数默认为 `*` ，表示允许所有路径的 Jar 包。
   * 如果配置 `jdbc_driver_secure_path` 为空，则不允许所有路径的驱动包，也就意味着只能使用上述方式 1 来指定驱动包。

   > 如配置 `jdbc_driver_secure_path = "file:///path/to/jdbc_drivers;http://path/to/jdbc_drivers"`, 则只允许以 `file:///path/to/jdbc_drivers` 或 `http://path/to/jdbc_drivers` 开头的驱动包路径。

3. 在创建 Catalog 时，可以通过 `checksum` 参数来指定驱动包的校验和，Doris 会在加载驱动包后，对驱动包进行校验，如果校验失败，则会拒绝创建 Catalog。

:::warning
上述的校验只会在创建 Catalog 时进行，对于已经创建的 Catalog，不会再次进行校验。
:::

### 小写名称同步

当 `lower_case_meta_names` 设置为 `true` 时，Doris 通过维护小写名称到远程系统中实际名称的映射，使查询时能够使用小写去查询外部数据源非小写的数据库和表以及列。

由于 FE 存在 `lower_case_table_names` 的参数，会影响查询时的表名大小写规则，所以规则如下

* 当 FE `lower_case_table_names` config 为 0 时

   lower_case_meta_names = false，大小写和源库一致。
   lower_case_meta_names = true，小写存储库表列名。

* 当 FE `lower_case_table_names` config 为 1 时

   lower_case_meta_names = false，db 和 column 的大小写和源库一致，但是 table 存储为小写
   lower_case_meta_names = true，小写存储库表列名。

* 当 FE `lower_case_table_names` config 为 2 时

   lower_case_meta_names = false，大小写和源库一致。
   lower_case_meta_names = true，小写存储库表列名。

如果创建 Catalog 时的参数配置匹配到了上述规则中的转变小写规则，则 Doris 会将对应的名称转变为小写存储在 Doris 中，查询时需使用 Doris 显示的小写名称去查询。

如果外部数据源存在名称相同只有大小写不同的情况，例如 DORIS 和 doris，Doris 由于歧义而在查询 Catalog 时报错，此时需要配置 `meta_names_mapping` 参数来解决冲突。

`meta_names_mapping` 参数接受一个 Json 格式的字符串，格式如下：

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

在将此配置填写到创建 Catalog 的语句中时，Json 中存在双引号，因此在填写时需要将双引号转义或者直接使用单引号包裹 Json 字符串。

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



**注意：**

JDBC Catalog 对于外部表大小写的映射规则存在如下三个阶段：

* Doris 2.0.3 之前的版本

    此配置名为 `lower_case_table_names`，仅对 Oracle 数据库有效，如在其他数据源设置此参数为 `true` 会影响查询，请勿设置。
    
    在查询 Oracle 时，会将所有的库名和表名转换为大写，再去查询 Oracle，例如：

    Oracle 在 TEST 空间下有 TEST 表，Doris 创建 Catalog 时设置 `lower_case_table_names` 为 `true`，则 Doris 可以通过 `select * from oracle_catalog.test.test` 查询到 TEST 表，Doris 会自动将 test.test 格式化成 TEST.TEST 下发到 Oracle，需要注意的是这是个默认行为，也意味着不能查询 Oracle 中小写的表名。

* Doris 2.0.3 版本：

    此配置名为 `lower_case_table_names`，对所有的数据库都有效，在查询时，会将所有的库名和表名转换为真实的名称，再去查询，如果是从老版本升级到 2.0.3 ，需要 `Refresh <catalog_name>` 才能生效。

    但是，如果库名、表名或列名只有大小写不同，例如 `Doris` 和 `doris`，则 Doris 由于歧义而无法查询它们。

    并且当 FE 参数的 `lower_case_table_names` 设置为 `1` 或 `2` 时，JDBC Catalog 的 `lower_case_table_names` 参数必须设置为 `true`。如果 FE 参数的 `lower_case_table_names` 设置为 `0`，则 JDBC Catalog 的参数可以为 `true` 或 `false`，默认为 `false`。

* Doris 2.1.0 以及之后版本：

    为了避免和 FE conf 的 `lower_case_table_names` 参数混淆，此配置名改为 `lower_case_meta_names`，对所有的数据库都有效，在查询时，会将所有的库名和表名以及列名转换为真实的名称，再去查询，如果是从老版本升级到 2.1.0 ，需要 `Refresh <catalog_name>` 才能生效。

    具体规则参考本小节开始对于 `lower_case_meta_names` 的介绍。

    此前设置过 JDBC Catalog `lower_case_table_names` 参数的用户会在升级到 2.1.0 时，自动将 `lower_case_table_names` 转换为 `lower_case_meta_names`。

### 指定同步数据库

`only_specified_database`:
在jdbc连接时可以指定链接到哪个database/schema, 如：mysql中jdbc_url中可以指定database, pg的jdbc_url中可以指定currentSchema。

`include_database_list`:
仅在`only_specified_database=true`时生效，指定需要同步的 database，以','分割，db名称是大小写敏感的。

`exclude_database_list`:
仅在`only_specified_database=true`时生效，指定不需要同步的多个database，以','分割，db名称是大小写敏感的。

当 `include_database_list` 和 `exclude_database_list` 有重合的database配置时，`exclude_database_list`会优先生效。

如果使用该参数时连接oracle数据库，要求使用ojdbc8.jar以上版本jar包。

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
   
2. 当变量 `enable_ext_func_pred_pushdown` 设置为true，会将 where 之后的函数条件也下推到外部数据源，目前仅支持 MySQL、ClickHouse、Oracle，如遇到 MySQL、ClickHouse、Oracle 不支持的函数，可以将此参数设置为 false，目前 Doris 会自动识别部分 MySQL 不支持的函数以及 CLickHouse、Oracle 支持的函数进行下推条件过滤，可通过 explain sql 查看。

目前不会下推的函数有：

|    MYSQL     |
|:------------:|
|  DATE_TRUNC  |
| MONEY_FORMAT |

目前会下推的函数有：

|   ClickHouse   |
|:--------------:|
| FROM_UNIXTIME  |
| UNIX_TIMESTAMP |

| Oracle |
|:------:|
|  NVL   |

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

## JDBC 连接池配置

在 Doris 中，每个 FE 和 BE 节点都会维护一个连接池，这样可以避免频繁地打开和关闭单独的数据源连接。连接池中的每个连接都可以用来与数据源建立连接并执行查询。任务完成后，这些连接会被归还到池中以便重复使用，这不仅提高了性能，还减少了建立连接时的系统开销，并帮助防止达到数据源的连接数上限。

下面列出了一些可用于调整连接池行为的 Catalog 配置属性：

| 参数名称                            | 默认值     | 描述和行为                                                                                               |
|---------------------------------|---------|-----------------------------------------------------------------------------------------------------|
| `connection_pool_min_size`      | 1       | 定义连接池的最小连接数，用于初始化连接池并保证在启用保活机制时至少有该数量的连接处于活跃状态。                                                     |
| `connection_pool_max_size`      | 10      | 定义连接池的最大连接数，每个 Catalog 对应的每个 FE 或 BE 节点最多可持有此数量的连接。                                                 |
| `connection_pool_max_wait_time` | 5000    | 如果连接池中没有可用连接，定义客户端等待连接的最大毫秒数。                                                                       |
| `connection_pool_max_life_time` | 1800000 | 设置连接在连接池中保持活跃的最大时长（毫秒）。超时的连接将被回收。同时，此值的一半将作为连接池的最小逐出空闲时间，达到该时间的连接将成为逐出候选对象。执行回收操作的频率将是该最大生命周期的十分之一。 |
| `connection_pool_keep_alive`    | false   | 仅在 BE 节点上有效，用于决定是否保持达到最小逐出空闲时间但未到最大生命周期的连接活跃。默认关闭，以减少不必要的资源使用。                                      |

为了避免在 BE 上累积过多的未使用的连接池缓存，可以通过设置 BE 的 `jdbc_connection_pool_cache_clear_time_sec` 参数来指定清理缓存的时间间隔。默认值为 28800 秒（8小时），此间隔过后，BE 将强制清理所有超过该时间未使用的连接池缓存。

## 使用指南

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

### SQL 透传

在 Doris 2.0.3 之前的版本中，用户只能通过 JDBC Catalog 进行查询操作（SELECT）。
在 Doris 2.0.4 版本之后，用户可以通过 `CALL` 命令，对 JDBC 数据源进行 DDL 和 DML 操作。

```
CALL EXECUTE_STMT("catalog_name", "raw_stmt_string");
```

`EXECUTE_STMT()` 过程有两个参数：

- Catalog Name：目前仅支持 Jdbc Catalog。
- 执行语句：目前仅支持 DDL 和 DML 语句。并且需要直接使用 JDBC 数据源对应的语法。

```
CALL EXECUTE_STMT("jdbc_catalog", "insert into db1.tbl1 values(1,2), (3, 4)");

CALL EXECUTE_STMT(jdbc_catalog", "delete from db1.tbl1 where k1 = 2");

CALL EXECUTE_STMT(jdbc_catalog", "create table dbl1.tbl2 (k1 int)");
```

#### 原理和限制

通过 `CALL EXECUTE_STMT()` 命令，Doris 会直接将用户编写的 SQL 语句发送给 Catalog 对应的 JDBC 数据源进行执行。因此，这个操作有如下限制：

- SQL 语句必须是数据源对应的语法，Doris 不会做语法和语义检查。
- SQL 语句中引用的表名建议是全限定名，即 `db.tbl` 这种格式。如果未指定 db，则会使用 JDBC Catalog 的 JDBC url 中指定的 db 名称。
- SQL 语句中不可引用 JDBC 数据源之外的库表，也不可以引用 Doris 的库表。但可以引用在 JDBC 数据源内的，但是没有同步到 Doris JDBC Catalog 的库表。
- 执行 DML 语句，无法获取插入、更新或删除的行数，只能获取命令是否执行成功。
- 只有对 Catalog 有 LOAD 权限的用户，才能执行这个命令。

## 支持的数据源

### MySQL

#### 创建示例

* mysql 5.7

    ```sql
    CREATE CATALOG jdbc_mysql PROPERTIES (
        "type"="jdbc",
        "user"="root",
        "password"="123456",
        "jdbc_url" = "jdbc:mysql://127.0.0.1:3306/demo",
        "driver_url" = "mysql-connector-java-5.1.49.jar",
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

| MYSQL Type                                | Doris Type              | Comment                                               |
|-------------------------------------------|-------------------------|-------------------------------------------------------|
| BOOLEAN                                   | TINYINT                 |                                                       |
| TINYINT                                   | TINYINT                 |                                                       |
| SMALLINT                                  | SMALLINT                |                                                       |
| MEDIUMINT                                 | INT                     |                                                       |
| INT                                       | INT                     |                                                       |
| BIGINT                                    | BIGINT                  |                                                       |
| UNSIGNED TINYINT                          | SMALLINT                | Doris 没有 UNSIGNED 数据类型，所以扩大一个数量级            |
| UNSIGNED MEDIUMINT                        | INT                     | Doris 没有 UNSIGNED 数据类型，所以扩大一个数量级            |
| UNSIGNED INT                              | BIGINT                  | Doris 没有 UNSIGNED 数据类型，所以扩大一个数量级            |
| UNSIGNED BIGINT                           | LARGEINT                |                                                       |
| FLOAT                                     | FLOAT                   |                                                       |
| DOUBLE                                    | DOUBLE                  |                                                       |
| DECIMAL                                   | DECIMAL                 |                                                       |
| UNSIGNED DECIMAL(p,s)                     | DECIMAL(p+1,s) / STRING | 如果p+1>38, 将使用Doris STRING类型                       |
| DATE                                      | DATE                    |                                                       |
| TIMESTAMP                                 | DATETIME                |                                                       |
| DATETIME                                  | DATETIME                |                                                       |
| YEAR                                      | SMALLINT                |                                                       |
| TIME                                      | STRING                  |                                                       |
| CHAR                                      | CHAR                    |                                                       |
| VARCHAR                                   | VARCHAR                 |                                                       |
| JSON                                      | STRING                  | 为了更好的性能，将外部数据源的 JSON 映射为 STRING 而不是JSONB |
| SET                                       | STRING                  |                                                       |
| BIT                                       | BOOLEAN/STRING          | BIT(1) 会映射为 BOOLEAN,其他 BIT 映射为 STRING            |
| TINYTEXT、TEXT、MEDIUMTEXT、LONGTEXT         | STRING                  |                                                       |
| BLOB、MEDIUMBLOB、LONGBLOB、TINYBLOB         | STRING                  |                                                       |
| TINYSTRING、STRING、MEDIUMSTRING、LONGSTRING | STRING                  |                                                       |
| BINARY、VARBINARY                          | STRING                  |                                                       |
| Other                                     | UNSUPPORTED             |                                                       |

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

 | POSTGRESQL Type                         | Doris Type     | Comment                                             |
 |-----------------------------------------|----------------|-----------------------------------------------------|
 | boolean                                 | BOOLEAN        |                                                     |
 | smallint/int2                           | SMALLINT       |                                                     |
 | integer/int4                            | INT            |                                                     |
 | bigint/int8                             | BIGINT         |                                                     |
 | decimal/numeric                         | DECIMAL        |                                                     |
 | real/float4                             | FLOAT          |                                                     |
 | double precision                        | DOUBLE         |                                                     |
 | smallserial                             | SMALLINT       |                                                     |
 | serial                                  | INT            |                                                     |
 | bigserial                               | BIGINT         |                                                     |
 | char                                    | CHAR           |                                                     |
 | varchar/text                            | STRING         |                                                     |
 | timestamp                               | DATETIME       |                                                     |
 | date                                    | DATE           |                                                     |
 | json/jsonb                              | STRING         | 为了更好的性能，将外部数据源的 JSON 映射为 STRING 而不是JSONB|
 | time                                    | STRING         |                                                     |
 | interval                                | STRING         |                                                     |
 | point/line/lseg/box/path/polygon/circle | STRING         |                                                     |
 | cidr/inet/macaddr                       | STRING         |                                                     |
 | bit                                     | BOOLEAN/STRING | bit(1)会映射为 BOOLEAN,其他 bit 映射为 STRING           |
 | uuid                                    | STRING         |                                                     |
 | Other                                   | UNSUPPORTED    |                                                     |

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
| timestamp                              | STRING        | 读取二进制数据的十六进制显示，无实际意义                            |
| Other                                  | UNSUPPORTED   |                                                              |

### Doris

Jdbc Catalog 也支持连接另一个Doris数据库：

* mysql 5.7 Driver

```sql
CREATE CATALOG jdbc_doris PROPERTIES (
    "type"="jdbc",
    "user"="root",
    "password"="123456",
    "jdbc_url" = "jdbc:mysql://127.0.0.1:9030?useSSL=false",
    "driver_url" = "mysql-connector-java-5.1.49.jar",
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
| HLL        | HLL                    | 查询HLL需要设置`return_object_data_as_binary=true`     |
| Array      | Array                  | Array内部类型适配逻辑参考上述类型，不支持嵌套复杂类型        |
| BITMAP     | BITMAP                 | 查询BITMAP需要设置`return_object_data_as_binary=true`  |
| Other      | UNSUPPORTED            |                                                      |

### ClickHouse

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
 Doris 在连接 OceanBase 时，会自动识别 OceanBase 处于 MySQL 或者 Oracle 模式，层级对应和类型映射参考 [MySQL](#mysql) 与 [Oracle](#oracle)
:::

### DB2

#### 创建示例

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

#### 层级映射

映射 DB2 时，Doris 的 Database 对应于 DB2 中指定 DataBase（如示例中 `jdbc_url` 参数中的 "doris"）下的一个 Schema。而 Doris 的 Database 下的 Table 则对应于 DB2 中 Schema 下的 Tables。即映射关系如下：

|  Doris   |   DB2    |
|:--------:|:--------:|
| Catalog  | DataBase |
| Database |  Schema  |
|  Table   |  Table   |


#### 类型映射

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
| XML              | STRING       |       |
| OTHER            | UNSUPPORTED  |       |

## JDBC Driver 列表

推荐使用以下版本的 Driver 连接对应的数据库。其他版本的 Driver 未经测试，可能导致非预期的问题。

|    Source    |                        JDBC Driver Version                        |
|:------------:|:-----------------------------------------------------------------:|
|  MySQL 5.x   |                  mysql-connector-java-5.1.49.jar                  |
|  MySQL 8.x   |                  mysql-connector-java-8.0.25.jar                  |
|  PostgreSQL  |                       postgresql-42.5.1.jar                       |
|    Oracle    |                            ojdbc8.jar                             |
|  SQLServer   |                    mssql-jdbc-11.2.3.jre8.jar                     |
|    Doris     | mysql-connector-java-5.1.49.jar / mysql-connector-java-8.0.25.jar |
|  Clickhouse  |                   clickhouse-jdbc-0.4.2-all.jar                   |
|   SAP HAHA   |                             ngdbc.jar                             |
| Trino/Presto |            trino-jdbc-389.jar / presto-jdbc-0.280.jar             |
|  OceanBase   |                    oceanbase-client-2.4.2.jar                     |
|     DB2      |                         jcc-11.5.8.0.jar                          |

## 常见问题

1. 除了 MySQL,Oracle,PostgreSQL,SQLServer,ClickHouse,SAP HANA,Trino/Presto,OceanBase,DB2 是否能够支持更多的数据库

    目前Doris只适配了 MySQL,Oracle,PostgreSQL,SQLServer,ClickHouse,SAP HANA,Trino/Presto,OceanBase,DB2. 关于其他的数据库的适配工作正在规划之中，原则上来说任何支持JDBC访问的数据库都能通过JDBC外表来访问。如果您有访问其他外表的需求，欢迎修改代码并贡献给Doris。

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

3. 读取 MySQL date/datetime 类型出现异常

    ```
    ERROR 1105 (HY000): errCode = 2, detailMessage = (10.16.10.6)[INTERNAL_ERROR]UdfRuntimeException: get next block failed: 
    CAUSED BY: SQLException: Zero date value prohibited
    CAUSED BY: DataReadException: Zero date value prohibited
    ```

    这是因为JDBC中对于该非法的 Date/DateTime 默认处理为抛出异常，可以通过参数 `zeroDateTimeBehavior`控制该行为。

    可选参数为: `EXCEPTION`,`CONVERT_TO_NULL`,`ROUND`, 分别为：异常报错，转为NULL值，转为 "0001-01-01 00:00:00";

    需要在创建 Catalog 的 `jdbc_url` 把JDBC连接串最后增加 `zeroDateTimeBehavior=convertToNull` ,如 `"jdbc_url" = "jdbc:mysql://127.0.0.1:3306/test?zeroDateTimeBehavior=convertToNull"`
    这种情况下，JDBC 会把 0000-00-00 或者 0000-00-00 00:00:00 转换成 null，然后 Doris 会把当前 Catalog 的所有 Date/DateTime 类型的列按照可空类型处理，这样就可以正常读取了。

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

6. 使用JDBC查询MYSQL大数据量时，如果查询偶尔能够成功，偶尔会报如下错误，且出现该错误时MYSQL的连接被全部断开，无法连接到MYSQL SERVER，过段时间后mysql又恢复正常，但是之前的连接都没了：

    ```
    ERROR 1105 (HY000): errCode = 2, detailMessage = [INTERNAL_ERROR]UdfRuntimeException: JDBC executor sql has error:
    CAUSED BY: CommunicationsException: Communications link failure
    The last packet successfully received from the server was 4,446 milliseconds ago. The last packet sent successfully to the server was 4,446 milliseconds ago.
    ```

    出现上述现象时，可能是Mysql Server自身的内存或CPU资源被耗尽导致Mysql服务不可用，可以尝试增大Mysql Server的内存或CPU配置。
 
7. 使用JDBC查询MYSQL的过程中，如果发现和在MYSQL库的查询结果不一致的情况

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

8. 读取 SQLServer 出现通信链路异常

    ```
    ERROR 1105 (HY000): errCode = 2, detailMessage = (10.16.10.6)[CANCELLED][INTERNAL_ERROR]UdfRuntimeException: Initialize datasource failed:
    CAUSED BY: SQLServerException: The driver could not establish a secure connection to SQL Server by using Secure Sockets Layer (SSL) encryption.
    Error: "sun.security.validator.ValidatorException: PKIX path building failed: sun.security.provider.certpath.SunCertPathBuilderException:
    unable to find valid certification path to requested target". ClientConnectionId:a92f3817-e8e6-4311-bc21-7c66
    ```

    可在创建 Catalog 的 `jdbc_url` 把JDBC连接串最后增加 `encrypt=false` ,如 `"jdbc_url" = "jdbc:sqlserver://127.0.0.1:1433;DataBaseName=doris_test;encrypt=false"`

9. 读取 Oracle 出现 `Non supported character set (add orai18n.jar in your classpath): ZHS16GBK` 异常
    
    下载 [orai18n.jar](https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html) 并放到 Doris FE 的 lib 目录以及 BE 的 lib/java_extensions 目录 (Doris 2.0 之前的版本需放到 BE 的 lib 目录下) 下即可。

    从 2.0.2 版本起，可以将这个文件放置在 FE 和 BE 的 `custom_lib/` 目录下（如不存在，手动创建即可），以防止升级集群时因为 lib 目录被替换而导致文件丢失。

10. 通过jdbc catalog 读取Clickhouse数据出现`NoClassDefFoundError: net/jpountz/lz4/LZ4Factory` 错误信息
    
    可以先下载[lz4-1.3.0.jar](https://repo1.maven.org/maven2/net/jpountz/lz4/lz4/1.3.0/lz4-1.3.0.jar)包，然后放到DorisFE lib 目录以及BE 的 `lib/lib/java_extensions`目录中（Doris 2.0 之前的版本需放到 BE 的 lib 目录下）。

    从 2.0.2 版本起，可以将这个文件放置在 FE 和 BE 的 `custom_lib/` 目录下（如不存在，手动创建即可），以防止升级集群时因为 lib 目录被替换而导致文件丢失。

11. 如果通过 Jdbc catalog 查询 MySQL 的时候，出现长时间卡住没有返回结果，或着卡住很长时间并且 fe.warn.log 中出现出现大量 write lock 日志，可以尝试在 url 添加 socketTimeout ，例如：`jdbc:mysql://host:port/database?socketTimeout=30000` ， 防止 MySQL 在关闭连接后 Jdbc 客户端无限等待。

