--- 
{
    "title": "High-Concurrency Point Query",
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

# High-Concurrency Point Query

<version since="2.0.0"></version>

## Background 

Doris is built on a columnar storage format engine. In high-concurrency service scenarios, users always want to retrieve entire rows of data from the system. However, when tables are wide, the columnar format greatly amplifies random read IO. Doris query engine and planner are too heavy for some simple queries, such as point queries. A short path needs to be planned in the FE's query plan to handle such queries. FE is the access layer service for SQL queries, written in Java. Parsing and analyzing SQL also leads to high CPU overhead for high-concurrency queries. To solve these problems, we have introduced row storage, short query path, and PreparedStatement in Doris. Below is a guide to enable these optimizations.

## Row Store Format

We support a row format for olap table to reduce point lookup io cost,
but to enable this format, you need to spend more disk space for row format store.
Currently, we store row in an extra column called `row column` for simplicity.
The Row Storage mode can only be turned on when creating a table. You need to specify the following properties in the property of the table creation statement:

```
"store_row_column" = "true"
```

## Accelerate point query for unique model

The above row storage is used to enable the Merge-On-Write strategy under the Unique model to reduce the IO overhead during enumeration. When `enable_unique_key_merge_on_write` and `store_row_column` are enabled when creating a Unique table, the query of the primary key will take a short path to optimize SQL execution, and only one RPC is required to complete the query. The following is an example of enabling the Merge-On-Write strategy under the Unique model by combining the query and row existence:

```sql
CREATE TABLE `tbl_point_query` (
    `key` int(11) NULL,
    `v1` decimal(27, 9) NULL,
    `v2` varchar(30) NULL,
    `v3` varchar(30) NULL,
    `v4` date NULL,
    `v5` datetime NULL,
    `v6` float NULL,
    `v7` datev2 NULL
) ENGINE=OLAP
UNIQUE KEY(`key`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`key)` BUCKETS 1
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "enable_unique_key_merge_on_write" = "true",
    "light_schema_change" = "true",
    "store_row_column" = "true"
);
```

**Note:**
1. `enable_unique_key_merge_on_write` should be enabled, since we need primary key for quick point lookup in storage engine
2. when condition only contains primary key like `select * from tbl_point_query where key = 123`, such query will go through the short fast path
3. `light_schema_change` should also been enabled since we rely on `column unique id` of each column when doing a point query.
4. It only supports equality queries on the key column of a single table and does not support joins or nested subqueries. The WHERE condition should consist of the key column alone and be an equality comparison. It can be considered as a type of key-value query.


## Using `PreparedStatement`

In order to reduce CPU cost for parsing query SQL and SQL expressions, we provide `PreparedStatement` feature in FE fully compatible with mysql protocol (currently only support point queries like above mentioned).Enable it will pre caculate PreparedStatement SQL and expresions and caches it in a session level memory buffer and will be reused later on.We could improve 4x+ performance by using `PreparedStatement` when CPU became hotspot doing such queries.Bellow is an JDBC example of using `PreparedStatement`.

1. Setup JDBC url and enable server side prepared statement

```
url = jdbc:mysql://127.0.0.1:9030/ycsb?useServerPrepStmts=true
```

2. Using `PreparedStatement`

```java
// use `?` for placement holders, readStatement should be reused
PreparedStatement readStatement = conn.prepareStatement("select * from tbl_point_query where key = ?");
...
readStatement.setInt(1234);
ResultSet resultSet = readStatement.executeQuery();
...
readStatement.setInt(1235);
resultSet = readStatement.executeQuery();
...
```

## Enable row cache
Doris has a page-level cache that stores data for a specific column in each page. Therefore, the page cache is a column-based cache. For the row storage mentioned earlier, a row contains data for multiple columns, and the cache may be evicted by large queries, which can reduce the hit rate. To increase the hit rate of the row cache, a separate row cache is introduced, which reuses the LRU cache mechanism in Doris to ensure memory usage. You can enable it by specifying the following BE configuration:

- `disable_storage_row_cache` : Whether to enable the row cache. It is not enabled by default.
- `row_cache_mem_limit` : Specifies the percentage of memory occupied by the row cache. The default is 20% of memory.

