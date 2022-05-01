---
{
    "title": "Doris On Hive",
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

# Hive External Table of Doris

Hive External Table of Doris 提供了 Doris 直接访问 Hive 外部表的能力，外部表省去了繁琐的数据导入工作，并借助 Doris 本身的 OLAP 的能力来解决 Hive 表的数据分析问题：

1. 支持 Hive 数据源接入Doris
2. 支持 Doris 与 Hive 数据源中的表联合查询，进行更加复杂的分析操作

本文档主要介绍该功能的使用方式和注意事项等。

## 名词解释

### Doris 相关

* FE：Frontend，Doris 的前端节点,负责元数据管理和请求接入
* BE：Backend，Doris 的后端节点,负责查询执行和数据存储

## 使用方法

### Doris 中创建 Hive 的外表

具体建表语法参照：[CREATE TABLE](../../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE.html)

```sql
-- 语法
CREATE [EXTERNAL] TABLE table_name (
  col_name col_type [NULL | NOT NULL] [COMMENT "comment"]
) ENGINE=HIVE
[COMMENT "comment"]
PROPERTIES (
  'property_name'='property_value',
  ...
);

-- 例子：创建 Hive 集群中 hive_db 下的 hive_table 表
CREATE TABLE `t_hive` (
  `k1` int NOT NULL COMMENT "",
  `k2` char(10) NOT NULL COMMENT "",
  `k3` datetime NOT NULL COMMENT "",
  `k5` varchar(20) NOT NULL COMMENT "",
  `k6` double NOT NULL COMMENT ""
) ENGINE=HIVE
COMMENT "HIVE"
PROPERTIES (
'hive.metastore.uris' = 'thrift://192.168.0.1:9083',
'database' = 'hive_db',
'table' = 'hive_table'
);
```

#### 参数说明：

- 外表列
    - 列名要于 Hive 表一一对应
    - 列的顺序需要与 Hive 表一致
    - 必须包含 Hive 表中的全部列
    - Hive 表分区列无需指定，与普通列一样定义即可。
- ENGINE 需要指定为 HIVE
- PROPERTIES 属性：
    - `hive.metastore.uris`：Hive Metastore 服务地址
    - `database`：挂载 Hive 对应的数据库名
    - `table`：挂载 Hive 对应的表名

## 类型匹配

支持的 Hive 列类型与 Doris 对应关系如下表：

|  Hive  | Doris  |             描述              |
| :------: | :----: | :-------------------------------: |
|   BOOLEAN  | BOOLEAN  |                         |
|   CHAR   |  CHAR  |            当前仅支持UTF8编码            |
|   VARCHAR | VARCHAR |       当前仅支持UTF8编码       |
|   TINYINT   | TINYINT |  |
|   SMALLINT  | SMALLINT |  |
|   INT  | INT |  |
|   BIGINT  | BIGINT |  |
|   FLOAT   |  FLOAT  |                                   |
|   DOUBLE  | DOUBLE |  |
|   DECIMAL  | DECIMAL |  |
|   DATE   |  DATE  |                                   |
|   TIMESTAMP  | DATETIME | Timestamp 转成 Datetime 会损失精度 |

**注意：**
- Hive 表 Schema 变更**不会自动同步**，需要在 Doris 中重建 Hive 外表。
- 当前 Hive 的存储格式仅支持 Text，Parquet 和 ORC 类型
- 当前默认支持的 Hive 版本为 `2.3.7、3.1.2`，未在其他版本进行测试。后续后支持更多版本。

### 查询用法

完成在 Doris 中建立 Hive 外表后，除了无法使用 Doris 中的数据模型(rollup、预聚合、物化视图等)外，与普通的 Doris OLAP 表并无区别

```sql
select * from t_hive where k1 > 1000 and k3 ='term' or k4 like '%doris';
```



