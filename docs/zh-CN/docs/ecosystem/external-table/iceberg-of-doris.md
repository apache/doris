---
{
    "title": "Doris On Iceberg",
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

# Iceberg External Table of Doris

Iceberg External Table of Doris 提供了 Doris 直接访问 Iceberg 外部表的能力，外部表省去了繁琐的数据导入工作，并借助 Doris 本身的 OLAP 的能力来解决 Iceberg 表的数据分析问题：

1. 支持 Iceberg 数据源接入Doris
2. 支持 Doris 与 Iceberg 数据源中的表联合查询，进行更加复杂的分析操作

本文档主要介绍该功能的使用方式和注意事项等。

## 名词解释

### Doris 相关

* FE：Frontend，Doris 的前端节点,负责元数据管理和请求接入
* BE：Backend，Doris 的后端节点,负责查询执行和数据存储

## 使用方法

### Doris 中创建 Iceberg 的外表

可以通过以下两种方式在 Doris 中创建 Iceberg 外表。建外表时无需声明表的列定义，Doris 可以根据 Iceberg 中表的列定义自动转换。

1. 创建一个单独的外表，用于挂载 Iceberg 表。  
   具体相关语法，可以通过 [CREATE TABLE](../../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE.md) 查看。

    ```sql
    -- 语法
    CREATE [EXTERNAL] TABLE table_name 
    ENGINE = ICEBERG
    [COMMENT "comment"]
    PROPERTIES (
    "iceberg.database" = "iceberg_db_name",
    "iceberg.table" = "icberg_table_name",
    "iceberg.hive.metastore.uris"  =  "thrift://192.168.0.1:9083",
    "iceberg.catalog.type"  =  "HIVE_CATALOG"
    );


    -- 例子1：挂载 Iceberg 中 iceberg_db 下的 iceberg_table
    CREATE TABLE `t_iceberg` 
    ENGINE = ICEBERG
    PROPERTIES (
    "iceberg.database" = "iceberg_db",
    "iceberg.table" = "iceberg_table",
    "iceberg.hive.metastore.uris"  =  "thrift://192.168.0.1:9083",
    "iceberg.catalog.type"  =  "HIVE_CATALOG"
    );


    -- 例子2：挂载 Iceberg 中 iceberg_db 下的 iceberg_table，HDFS开启HA
    CREATE TABLE `t_iceberg`
    ENGINE = ICEBERG
    PROPERTIES (
    "iceberg.database" = "iceberg_db",
    "iceberg.table" = "iceberg_table",
    "iceberg.hive.metastore.uris"  =  "thrift://192.168.0.1:9083",
    "iceberg.catalog.type"  =  "HIVE_CATALOG",
    "dfs.nameservices"="HDFS8000463",
    "dfs.ha.namenodes.HDFS8000463"="nn2,nn1",
    "dfs.namenode.rpc-address.HDFS8000463.nn2"="172.21.16.5:4007",
    "dfs.namenode.rpc-address.HDFS8000463.nn1"="172.21.16.26:4007",
    "dfs.client.failover.proxy.provider.HDFS8000463"="org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
    );
    ```

2. 创建一个 Iceberg 数据库，用于挂载远端对应 Iceberg 数据库，同时挂载该 database 下的所有 table。  
   具体相关语法，可以通过 [CREATE DATABASE](../../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-DATABASE.md) 查看。

    ```sql
    -- 语法
    CREATE DATABASE db_name 
    [COMMENT "comment"]
    PROPERTIES (
    "iceberg.database" = "iceberg_db_name",
    "iceberg.hive.metastore.uris" = "thrift://192.168.0.1:9083",
    "iceberg.catalog.type" = "HIVE_CATALOG"
    );

    -- 例子：挂载 Iceberg 中的 iceberg_db，同时挂载该 db 下的所有 table
    CREATE DATABASE `iceberg_test_db`
    PROPERTIES (
    "iceberg.database" = "iceberg_db",
    "iceberg.hive.metastore.uris" = "thrift://192.168.0.1:9083",
    "iceberg.catalog.type" = "HIVE_CATALOG"
    );
    ```

   `iceberg_test_db` 中的建表进度可以通过 `HELP SHOW TABLE CREATION` 查看。

也可以根据自己的需求明确指定列定义来创建 Iceberg 外表。

1. 创一个 Iceberg 外表

    ```sql
    -- 语法
    CREATE [EXTERNAL] TABLE table_name (
        col_name col_type [NULL | NOT NULL] [COMMENT "comment"]
    ) ENGINE = ICEBERG
    [COMMENT "comment"]
    PROPERTIES (
    "iceberg.database" = "iceberg_db_name",
    "iceberg.table" = "icberg_table_name",
    "iceberg.hive.metastore.uris"  =  "thrift://192.168.0.1:9083",
    "iceberg.catalog.type"  =  "HIVE_CATALOG"
    );

    -- 例子1：挂载 Iceberg 中 iceberg_db 下的 iceberg_table
    CREATE TABLE `t_iceberg` (
        `id` int NOT NULL COMMENT "id number",
        `name` varchar(10) NOT NULL COMMENT "user name"
    ) ENGINE = ICEBERG
    PROPERTIES (
    "iceberg.database" = "iceberg_db",
    "iceberg.table" = "iceberg_table",
    "iceberg.hive.metastore.uris"  =  "thrift://192.168.0.1:9083",
    "iceberg.catalog.type"  =  "HIVE_CATALOG"
    );

    -- 例子2：挂载 Iceberg 中 iceberg_db 下的 iceberg_table，HDFS开启HA
    CREATE TABLE `t_iceberg` (
        `id` int NOT NULL COMMENT "id number",
        `name` varchar(10) NOT NULL COMMENT "user name"
    ) ENGINE = ICEBERG
    PROPERTIES (
    "iceberg.database" = "iceberg_db",
    "iceberg.table" = "iceberg_table",
    "iceberg.hive.metastore.uris"  =  "thrift://192.168.0.1:9083",
    "iceberg.catalog.type"  =  "HIVE_CATALOG",
    "dfs.nameservices"="HDFS8000463",
    "dfs.ha.namenodes.HDFS8000463"="nn2,nn1",
    "dfs.namenode.rpc-address.HDFS8000463.nn2"="172.21.16.5:4007",
    "dfs.namenode.rpc-address.HDFS8000463.nn1"="172.21.16.26:4007",
    "dfs.client.failover.proxy.provider.HDFS8000463"="org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
    );
    ```

#### 参数说明：

- 外表列
    - 列名要于 Iceberg 表一一对应
    - 列的顺序需要与 Iceberg 表一致
- ENGINE 需要指定为 ICEBERG
- PROPERTIES 属性：
    - `iceberg.hive.metastore.uris`：Hive Metastore 服务地址
    - `iceberg.database`：挂载 Iceberg 对应的数据库名
    - `iceberg.table`：挂载 Iceberg 对应的表名，挂载 Iceberg database 时无需指定。
    - `iceberg.catalog.type`：Iceberg 中使用的 catalog 方式，默认为 `HIVE_CATALOG`，当前仅支持该方式，后续会支持更多的 Iceberg catalog 接入方式。

### 展示表结构

展示表结构可以通过 [SHOW CREATE TABLE](../../sql-manual/sql-reference/Show-Statements/SHOW-CREATE-TABLE.md) 查看。

### 同步挂载

当 Iceberg 表 Schema 发生变更时，可以通过 `REFRESH` 命令手动同步，该命令会将 Doris 中的 Iceberg 外表删除重建，具体帮助可以通过 `HELP REFRESH` 查看。

```sql
-- 同步 Iceberg 表
REFRESH TABLE t_iceberg;

-- 同步 Iceberg 数据库
REFRESH DATABASE iceberg_test_db;
```

## 类型匹配

支持的 Iceberg 列类型与 Doris 对应关系如下表：

|  Iceberg  | Doris  |             描述              |
| :------: | :----: | :-------------------------------: |
|   BOOLEAN  | BOOLEAN  |                         |
|   INTEGER   |  INT  |                       |
|   LONG | BIGINT |              |
|   FLOAT   | FLOAT |  |
|   DOUBLE  | DOUBLE |  |
|   DATE  | DATE |  |
|   TIMESTAMP   |  DATETIME  | Timestamp 转成 Datetime 会损失精度 |
|   STRING   |  STRING  |                                   |
|   UUID  | VARCHAR | 使用 VARCHAR 来代替 | 
|   DECIMAL  | DECIMAL |  |
|   TIME  | - | 不支持 |
|   FIXED  | - | 不支持 |
|   BINARY  | - | 不支持 |
|   STRUCT  | - | 不支持 |
|   LIST  | - | 不支持 |
|   MAP  | - | 不支持 |

**注意：**
- Iceberg 表 Schema 变更**不会自动同步**，需要在 Doris 中通过 `REFRESH` 命令同步 Iceberg 外表或数据库。
- 当前默认支持的 Iceberg 版本为 0.12.0、0.13.2，未在其他版本进行测试。后续后支持更多版本。

### 查询用法

完成在 Doris 中建立 Iceberg 外表后，除了无法使用 Doris 中的数据模型(rollup、预聚合、物化视图等)外，与普通的 Doris OLAP 表并无区别

```sql
select * from t_iceberg where k1 > 1000 and k3 ='term' or k4 like '%doris';
```

## 相关系统配置

### FE配置

下面几个配置属于 Iceberg 外表系统级别的配置，可以通过修改 `fe.conf` 来配置，也可以通过 `ADMIN SET CONFIG` 来配置。

- `iceberg_table_creation_strict_mode`

  创建 Iceberg 表默认开启 strict mode。  
  strict mode 是指对 Iceberg 表的列类型进行严格过滤，如果有 Doris 目前不支持的数据类型，则创建外表失败。

- `iceberg_table_creation_interval_second`

  自动创建 Iceberg 表的后台任务执行间隔，默认为 10s。

- `max_iceberg_table_creation_record_size`

  Iceberg 表创建记录保留的最大值，默认为 2000. 仅针对创建 Iceberg 数据库记录。
