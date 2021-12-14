---
{
    "title": "Iceberg of Doris",
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
   具体相关语法，可以通过 `HELP CREATE TABLE` 查看。

    ```sql
    -- 语法
    CREATE [EXTERNAL] TABLE table_name 
    ENGINE = ICEBERG
    [COMMENT "comment"]
    PROPERTIES (
    "database" = "iceberg_db_name",
    "table" = "icberg_table_name",
    "hive.metastore.uris"  =  "thrift://192.168.0.1:9083",
    "catalog.type"  =  "HIVE_CATALOG"
    );


    -- 例子：挂载 Iceberg 中 iceberg_db 下的 iceberg_table 
    CREATE TABLE `t_iceberg` 
    ENGINE = ICEBERG
    PROPERTIES (
    "database" = "iceberg_db",
    "table" = "iceberg_table",
    "hive.metastore.uris"  =  "thrift://192.168.0.1:9083",
    "catalog.type"  =  "HIVE_CATALOG"
    );
    ```

2. 创建一个 Iceberg 数据库，用于挂载远端对应 Iceberg 数据库，同时挂载该 database 下的所有 table。  
   具体相关语法，可以通过 `HELP CREATE DATABASE` 查看。

    ```sql
    -- 语法
    CREATE DATABASE db_name 
    ENGINE = ICEBERG
    [COMMENT "comment"]
    PROPERTIES (
    "database" = "iceberg_db_name",
    "hive.metastore.uris" = "thrift://192.168.0.1:9083",
    "catalog.type" = "HIVE_CATALOG"
    );

    -- 例子：挂载 Iceberg 中的 iceberg_db，同时挂载该 db 下的所有 table
    CREATE DATABASE `iceberg_test_db`
    ENGINE = ICEBERG
    PROPERTIES (
    "database" = "iceberg_db",
    "hive.metastore.uris" = "thrift://192.168.0.1:9083",
    "catalog.type" = "HIVE_CATALOG"
    );
    ```

    `iceberg_test_db` 中的建表进度可以通过 `HELP SHOW TABLE CREATION` 查看。

#### 参数说明：

- ENGINE 需要指定为 ICEBERG
- PROPERTIES 属性：
    - `hive.metastore.uris`：Hive Metastore 服务地址
    - `database`：挂载 Iceberg 对应的数据库名
    - `table`：挂载 Iceberg 对应的表名，挂载 Iceberg database 时无需指定。
    - `catalog.type`：Iceberg 中使用的 catalog 方式，默认为 `HIVE_CATALOG`，当前仅支持该方式，后续会支持更多的 Iceberg catalog 接入方式。

### 展示表结构

展示表结构可以通过 `HELP SHOW CREATE TABLE` 查看。
    
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
- Iceberg 表 Schema 变更**不会自动同步**，需要在 Doris 中重建 Iceberg 外表或数据库。
- 当前默认支持的 Iceberg 版本为 0.12.0，未在其他版本进行测试。后续后支持更多版本。

### 查询用法

完成在 Doris 中建立 Iceberg 外表后，除了无法使用 Doris 中的数据模型(rollup、预聚合、物化视图等)外，与普通的 Doris OLAP 表并无区别

```sql
select * from t_iceberg where k1 > 1000 and k3 ='term' or k4 like '%doris';
```
