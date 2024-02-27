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
readStatement.setInt(1,1234);
ResultSet resultSet = readStatement.executeQuery();
...
readStatement.setInt(1,1235);
resultSet = readStatement.executeQuery();
...
```

## Enable row cache
Doris has a page-level cache that stores data for a specific column in each page. Therefore, the page cache is a column-based cache. For the row storage mentioned earlier, a row contains data for multiple columns, and the cache may be evicted by large queries, which can reduce the hit rate. To increase the hit rate of the row cache, a separate row cache is introduced, which reuses the LRU cache mechanism in Doris to ensure memory usage. You can enable it by specifying the following BE configuration:

- `disable_storage_row_cache` : Whether to enable the row cache. It is not enabled by default.
- `row_cache_mem_limit` : Specifies the percentage of memory occupied by the row cache. The default is 20% of memory.

## Performance optimization
1. Generally, it is effective to improve query processing capabilities by increasing the number of Observers.
2. Query load balancing: During the enumeration, if it is found that the FE CPU that accepts enumeration requests is used too high, or the request response becomes slow, you can use jdbc load balance for load balancing, and distribute the requests to multiple nodes to share the pressure (and also You can use other methods for query load balancing configuration, such as Nginx, proxySQL)
3. By directing the query requests to the Observer role to share the request pressure of high-concurrency queries and reducing the number of query requests sent to the fe master, it can usually solve the problem of the time-consuming fluctuation of the Fe Master node query to obtain better performance and stability

## QA
1. How to confirm that the configuration is correct and short path optimization using concurrent enumeration is used
   A: explain sql, when SHORT-CIRCUIT appears in the execution plan, it proves that short path optimization is used
   ```sql
   mysql> explain select * from tbl_point_query where `key` = -2147481418 ;                                                                                                                                
         +-----------------------------------------------------------------------------------------------+                                                                                                       
         | Explain String(Old Planner)                                                                   |                                                                                                       
         +-----------------------------------------------------------------------------------------------+                                                                                                       
         | PLAN FRAGMENT 0                                                                               |                                                                                                       
         |   OUTPUT EXPRS:                                                                               |                                                                                                       
         |     `test`.`tbl_point_query`.`key`                                                            |                                                                                                       
         |     `test`.`tbl_point_query`.`v1`                                                             |                                                                                                       
         |     `test`.`tbl_point_query`.`v2`                                                             |                                                                                                       
         |     `test`.`tbl_point_query`.`v3`                                                             |                                                                                                       
         |     `test`.`tbl_point_query`.`v4`                                                             |                                                                                                       
         |     `test`.`tbl_point_query`.`v5`                                                             |                                                                                                       
         |     `test`.`tbl_point_query`.`v6`                                                             |                                                                                                       
         |     `test`.`tbl_point_query`.`v7`                                                             |                                                                                                       
         |   PARTITION: UNPARTITIONED                                                                    |                                                                                                       
         |                                                                                               |                                                                                                       
         |   HAS_COLO_PLAN_NODE: false                                                                   |                                                                                                       
         |                                                                                               |                                                                                                       
         |   VRESULT SINK                                                                                |                                                                                                       
         |      MYSQL_PROTOCAL                                                                           |                                                                                                       
         |                                                                                               |                                                                                                       
         |   0:VOlapScanNode                                                                             |                                                                                                       
         |      TABLE: test.tbl_point_query(tbl_point_query), PREAGGREGATION: ON                         |                                                                                                       
         |      PREDICATES: `key` = -2147481418 AND `test`.`tbl_point_query`.`__DORIS_DELETE_SIGN__` = 0 |                                                                                                       
         |      partitions=1/1 (tbl_point_query), tablets=1/1, tabletList=360065                         |                                                                                                       
         |      cardinality=9452868, avgRowSize=833.31323, numNodes=1                                    |                                                                                                       
         |      pushAggOp=NONE                                                                           |                                                                                                       
         |      SHORT-CIRCUIT                                                                            |                                                                                                       
         +-----------------------------------------------------------------------------------------------+
      ```
2. How to confirm that prepared statement is effective
   A: After sending the request to Doris, find the corresponding query request in fe.audit.log and find Stmt=EXECUTE(), indicating that prepared statement is effective
   ```text
   2024-01-02 11:15:51,248 [query] |Client=192.168.1.82:53450|User=root|Db=test|State=EOF|ErrorCode=0|ErrorMessage=|Time(ms)=49|ScanBytes=0|ScanRows=0|ReturnRows=1|StmtId=51|QueryId=b63d30b908f04dad-ab4a
      3ba21d2c776b|IsQuery=true|isNereids=false|feIp=10.16.10.6|Stmt=EXECUTE(-2147481418)|CpuTimeMS=0|SqlHash=eee20fa2ac13a4f93bd4503a87921024|peakMemoryBytes=0|SqlDigest=|TraceId=|WorkloadGroup=|FuzzyVaria
      bles=
   ```
3. Can non-primary key queries use special optimization of high-concurrency point lookups?
   A: No, high-concurrency query only targets the equivalent query of the key column, and the query cannot contain join or nested subqueries.
4. Is useServerPrepStmts useful in ordinary queries?
   A: Prepared Statement currently only takes effect when primary key is checked.
5. Does optimizer selection require global settings?
   A: When using prepared statement for query, Doris will choose the query method with the best performance, and there is no need to manually set the optimizer.