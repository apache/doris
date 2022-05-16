---
{
    "title": "Doris Hudi external table",
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

# Hudi External Table of Doris

Hudi External Table of Doris 提供了 Doris 直接访问 Hudi 外部表的能力，外部表省去了繁琐的数据导入工作，并借助 Doris 本身的 OLAP 的能力来解决 Hudi 表的数据分析问题：

1. 支持 Hudi 数据源接入Doris
2. 支持 Doris 与 Hive数据源Hudi中的表联合查询，进行更加复杂的分析操作

本文档主要介绍该功能的使用方式和注意事项等。

## 名词解释

### Doris 相关

* FE：Frontend，Doris 的前端节点,负责元数据管理和请求接入
* BE：Backend，Doris 的后端节点,负责查询执行和数据存储

## 使用方法

### Doris 中创建 Hudi 的外表

可以通过以下两种方式在 Doris 中创建 Hudi 外表。建外表时无需声明表的列定义，Doris 可以在查询时从HiveMetaStore中获取列信息。

1. 创建一个单独的外表，用于挂载 Hudi 表。  
   具体相关语法，可以通过 [CREATE TABLE](../../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE.md) 查看。

    ```sql
    -- 语法
    CREATE [EXTERNAL] TABLE table_name
    [(column_definition1[, column_definition2, ...])]
    ENGINE = HUDI
    [COMMENT "comment"]
    PROPERTIES (
    "hudi.database" = "hudi_db_in_hive_metastore",
    "hudi.table" = "hudi_table_in_hive_metastore",
    "hudi.hive.metastore.uris" = "thrift://127.0.0.1:9083"
    );


    -- 例子：挂载 HiveMetaStore 中 hudi_db_in_hive_metastore 下的 hudi_table_in_hive_metastore，挂载时不指定schema。
    CREATE TABLE `t_hudi` 
    ENGINE = HUDI
    PROPERTIES (
    "hudi.database" = "hudi_db_in_hive_metastore",
    "hudi.table" = "hudi_table_in_hive_metastore",
    "hudi.hive.metastore.uris" = "thrift://127.0.0.1:9083"
    );

    -- 例子：挂载时指定schema
    CREATE TABLE `t_hudi` (
        `id` int NOT NULL COMMENT "id number",
        `name` varchar(10) NOT NULL COMMENT "user name"
    ) ENGINE = HUDI
    PROPERTIES (
    "hudi.database" = "hudi_db_in_hive_metastore",
    "hudi.table" = "hudi_table_in_hive_metastore",
    "hudi.hive.metastore.uris" = "thrift://127.0.0.1:9083"
    );
    ```


#### 参数说明：

- 外表列
    - 可以不指定列名，这时查询时会从HiveMetaStore中获取列信息，推荐这种建表方式
    - 指定列名时指定的列名要在 Hudi 表中存在
- ENGINE 需要指定为 HUDI
- PROPERTIES 属性：
    - `hudi.hive.metastore.uris`：Hive Metastore 服务地址
    - `hudi.database`：挂载 Hudi 对应的数据库名
    - `hudi.table`：挂载 Hudi 对应的表名

### 展示表结构

展示表结构可以通过 [SHOW CREATE TABLE](../../sql-manual/sql-reference/Show-Statements/SHOW-CREATE-TABLE.md) 查看。

## 类型匹配

支持的 Hudi 列类型与 Doris 对应关系如下表：

|  Hudi  | Doris  |             描述              |
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
- 当前默认支持的 Hudi 版本为 0.10.0，未在其他版本进行测试。后续后支持更多版本。

### 查询用法

完成在 Doris 中建立 Hudi 外表后，除了无法使用 Doris 中的数据模型(rollup、预聚合、物化视图等)外，与普通的 Doris OLAP 表并无区别

```sql
select * from t_hudi where k1 > 1000 and k3 ='term' or k4 like '%doris';
```
