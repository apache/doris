---
{
    "title": "通过外部表同步数据",
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

# 通过外部表同步数据

Doris 可以创建通过 ODBC 协议访问的外部表。创建完成后，可以通过 SELECT 语句直接查询外部表的数据，也可以通过 `INSERT INTO SELECT` 的方式导入外部表的数据。

本文档主要介绍如何创建通过 ODBC 协议访问的外部表，以及如何导入这些外部表的数据。目前支持的数据源包括：

- MySQL
- Oracle
- PostgreSQL
- SQLServer
- Hive(1.0版本支持)

## 创建外部表

创建 ODBC 外部表的详细介绍请参阅 [CREATE EXTERNAL TABLE](../../../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-EXTERNAL-TABLE.html) 语法帮助手册。

这里仅通过示例说明使用方式。

1. 创建 ODBC Resource

   ODBC Resource 的目的是用于统一管理外部表的连接信息。

   ```sql
   CREATE EXTERNAL RESOURCE `oracle_test_odbc`
   PROPERTIES (
       "type" = "odbc_catalog",
       "host" = "192.168.0.10",
       "port" = "8086",
       "user" = "oracle",
       "password" = "oracle",
       "database" = "oracle",
       "odbc_type" = "oracle",
       "driver" = "Oracle"
   );
   ```

这里我们创建了一个名为 `oracle_test_odbc` 的 Resource，其类型为 `odbc_catalog`，表示这是一个用于存储 ODBC 信息的 Resource。`odbc_type` 为 `oracle`，表示这个 OBDC Resource 是用于连接 Oracle 数据库的。关于其他类型的资源，具体可参阅 [资源管理](../../../advanced/resource.html) 文档。

2. 创建外部表

```sql
CREATE EXTERNAL TABLE `ext_oracle_demo` (
  `k1` decimal(9, 3) NOT NULL COMMENT "",
  `k2` char(10) NOT NULL COMMENT "",
  `k3` datetime NOT NULL COMMENT "",
  `k5` varchar(20) NOT NULL COMMENT "",
  `k6` double NOT NULL COMMENT ""
) ENGINE=ODBC
COMMENT "ODBC"
PROPERTIES (
    "odbc_catalog_resource" = "oracle_test_odbc",
    "database" = "oracle",
    "table" = "baseall"
);
```

这里我们创建一个 `ext_oracle_demo` 外部表，并引用了之前创建的 `oracle_test_odbc` Resource

## 导入数据

1. 创建 Doris 表

   这里我们创建一张 Doris 的表，列信息和上一步创建的外部表 `ext_oracle_demo` 一样：

   ```sql
   CREATE TABLE `doris_oralce_tbl` (
     `k1` decimal(9, 3) NOT NULL COMMENT "",
     `k2` char(10) NOT NULL COMMENT "",
     `k3` datetime NOT NULL COMMENT "",
     `k5` varchar(20) NOT NULL COMMENT "",
     `k6` double NOT NULL COMMENT ""
   )
   COMMENT "Doris Table"
   DISTRIBUTED BY HASH(k1) BUCKETS 2;
   PROPERTIES (
       "replication_num" = "1"
   );
   ```

   关于创建 Doris 表的详细说明，请参阅 [CREATE-TABLE](../../../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE.html) 语法帮助。

2. 导入数据 (从 `ext_oracle_demo`表 导入到 `doris_oralce_tbl` 表)

   

   ```sql
   INSERT INTO doris_oralce_tbl SELECT k1,k2,k3 FROM ext_oracle_demo limit 100;
   ```

   INSERT 命令是同步命令，返回成功，即表示导入成功。

## 注意事项

- 必须保证外部数据源与 Doris 集群是可以互通，包括BE节点和外部数据源的网络是互通的。
- ODBC 外部表本质上是通过单一 ODBC 客户端访问数据源，因此并不合适一次性导入大量的数据，建议分批多次导入。

## 更多帮助

关于 CREATE EXTERNAL TABLE 的更多详细语法和最佳实践，请参阅 [CREATE EXTERNAL TABLE](../../../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-EXTERNAL-TABLE.html) 命令手册。

Doris ODBC 更多使用示例请参考 [文章列表](https://doris.apache.org/zh-CN/article/article-list.html) 。
