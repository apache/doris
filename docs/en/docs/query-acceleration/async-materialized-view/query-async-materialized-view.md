---
{
    "title": "Querying Async Materialized View",
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

## Overview

Doris's asynchronous materialized views employ an algorithm based on the SPJG (SELECT-PROJECT-JOIN-GROUP-BY) pattern
structure information for transparent rewriting.

Doris can analyze the structural information of query SQL, automatically search for suitable materialized views, 
and attempt transparent rewriting, utilizing the optimal materialized view to express the query SQL.

By utilizing precomputed materialized view results, 
significant improvements in query performance and a reduction in computational costs can be achieved.

Using the three tables: lineitem, orders, and partsupp from TPC-H, let's describe the capability of directly querying
a materialized view and using the materialized view for transparent query rewriting.
```sql
CREATE TABLE IF NOT EXISTS lineitem (
    l_orderkey    integer not null,
    l_partkey     integer not null,
    l_suppkey     integer not null,
    l_linenumber  integer not null,
    l_quantity    decimalv3(15,2) not null,
    l_extendedprice  decimalv3(15,2) not null,
    l_discount    decimalv3(15,2) not null,
    l_tax         decimalv3(15,2) not null,
    l_returnflag  char(1) not null,
    l_linestatus  char(1) not null,
    l_shipdate    date not null,
    l_commitdate  date not null,
    l_receiptdate date not null,
    l_shipinstruct char(25) not null,
    l_shipmode     char(10) not null,
    l_comment      varchar(44) not null
    )
    DUPLICATE KEY(l_orderkey, l_partkey, l_suppkey, l_linenumber)
    PARTITION BY RANGE(l_shipdate)
    (FROM ('2023-10-17') TO ('2023-10-20') INTERVAL 1 DAY)
    DISTRIBUTED BY HASH(l_orderkey) BUCKETS 3
    PROPERTIES ("replication_num" = "1");

    insert into lineitem values
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (2, 4, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy'),
    (3, 2, 4, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-19', '2023-10-19', '2023-10-19', 'a', 'b', 'yyyyyyyyy');
```
```sql
CREATE TABLE IF NOT EXISTS orders  (
    o_orderkey       integer not null,
    o_custkey        integer not null,
    o_orderstatus    char(1) not null,
    o_totalprice     decimalv3(15,2) not null,
    o_orderdate      date not null,
    o_orderpriority  char(15) not null,
    o_clerk          char(15) not null,
    o_shippriority   integer not null,
    o_comment        varchar(79) not null
    )
    DUPLICATE KEY(o_orderkey, o_custkey)
    PARTITION BY RANGE(o_orderdate)(
    FROM ('2023-10-17') TO ('2023-10-20') INTERVAL 1 DAY)
    DISTRIBUTED BY HASH(o_orderkey) BUCKETS 3
    PROPERTIES ("replication_num" = "1");

    insert into orders values
    (1, 1, 'o', 9.5, '2023-10-17', 'a', 'b', 1, 'yy'),
    (1, 1, 'o', 10.5, '2023-10-18', 'a', 'b', 1, 'yy'),
    (2, 1, 'o', 11.5, '2023-10-19', 'a', 'b', 1, 'yy'),
    (3, 1, 'o', 12.5, '2023-10-19', 'a', 'b', 1, 'yy');
```

```sql
    CREATE TABLE IF NOT EXISTS partsupp (
      ps_partkey     INTEGER NOT NULL,
      ps_suppkey     INTEGER NOT NULL,
      ps_availqty    INTEGER NOT NULL,
      ps_supplycost  DECIMALV3(15,2)  NOT NULL,
      ps_comment     VARCHAR(199) NOT NULL 
    )
    DUPLICATE KEY(ps_partkey, ps_suppkey)
    DISTRIBUTED BY HASH(ps_partkey) BUCKETS 3
    PROPERTIES (
      "replication_num" = "1"
    );

    insert into partsupp values
    (2, 3, 9, 10.01, 'supply1'),
    (4, 3, 10, 11.01, 'supply2'),
    (2, 3, 10, 11.01, 'supply3');
```

## Direct Query of Materialized View
A materialized view can be considered as a table and can be queried just like a regular table.

The syntax for defining a materialized view, details can be found in 
[CREATE-ASYNC-MATERIALIZED-VIEW](../../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-ASYNC-MATERIALIZED-VIEW.md)

Materialized view definition:
```sql
CREATE MATERIALIZED VIEW mv1
BUILD IMMEDIATE REFRESH AUTO ON SCHEDULE EVERY 1 hour
DISTRIBUTED BY RANDOM BUCKETS 3
PROPERTIES ('replication_num' = '1')
AS
SELECT t1.l_linenumber,
       o_custkey,
       o_orderdate
FROM
    (SELECT * FROM lineitem WHERE l_linenumber > 1) t1
LEFT OUTER JOIN orders ON l_orderkey = o_orderkey;
```

Query statement:
Direct queries can be performed on the materialized view with additional filtering conditions and aggregations.
      
```sql
SELECT l_linenumber,
       o_custkey
FROM mv1
WHERE l_linenumber > 1 and o_orderdate = '2023-12-31';
```      

## Transparent Rewriting Capability
### Join rewriting


Join rewriting refers to when the tables used in the query and the materialization are the same. 
In this case, the optimizer will attempt transparent rewriting by either joining the input of the materialized 
view with the query or placing the join in the outer layer of the query's WHERE clause.

This pattern of rewriting is supported for multi-table joins and supports inner and left join types. 
Support for other types is continually expanding.

**Case 1:**

The following case can undergo transparent rewriting. The condition `l_linenumber > 1` allows for pull-up, 
enabling transparent rewriting by expressing the query using the precomputed results of the materialized view.

Materialized view definition:
```sql
CREATE MATERIALIZED VIEW mv2
BUILD IMMEDIATE REFRESH AUTO ON SCHEDULE EVERY 1 hour
DISTRIBUTED BY RANDOM BUCKETS 3
PROPERTIES ('replication_num' = '1')
AS
SELECT t1.l_linenumber,
       o_custkey,
       o_orderdate
FROM (SELECT * FROM lineitem WHERE l_linenumber > 1) t1
LEFT OUTER JOIN orders
ON l_orderkey = o_orderkey;
```
Query statement:

```sql
SELECT l_linenumber,
       o_custkey
FROM lineitem
LEFT OUTER JOIN orders
ON l_orderkey = o_orderkey
WHERE l_linenumber > 1 and o_orderdate = '2023-10-18';
```

**Case 2:**

JOIN Derivation occurs when the join type between the query and the materialized view does not match. 
In cases where the materialization can provide all the necessary data for the query, transparent rewriting can 
still be achieved by compensating predicates outside the join through predicate push down.

For example:

Materialized view definition:
```sql
CREATE MATERIALIZED VIEW mv3
BUILD IMMEDIATE REFRESH AUTO ON SCHEDULE EVERY 1 hour
DISTRIBUTED BY RANDOM BUCKETS 3
PROPERTIES ('replication_num' = '1')
AS
SELECT
    l_shipdate, l_suppkey, o_orderdate
    sum(o_totalprice) AS sum_total,
    max(o_totalprice) AS max_total,
    min(o_totalprice) AS min_total,
    count(*) AS count_all,
    count(distinct CASE WHEN o_shippriority > 1 AND o_orderkey IN (1, 3) THEN o_custkey ELSE null END) AS bitmap_union_basic
FROM lineitem
LEFT OUTER JOIN orders ON lineitem.l_orderkey = orders.o_orderkey AND l_shipdate = o_orderdate
GROUP BY
l_shipdate,
l_suppkey,
o_orderdate;
```

Query statement:
```sql
SELECT
    l_shipdate, l_suppkey, o_orderdate,
    sum(o_totalprice) AS sum_total,
    max(o_totalprice) AS max_total,
    min(o_totalprice) AS min_total,
    count(*) AS count_all,
    count(distinct CASE WHEN o_shippriority > 1 AND o_orderkey IN (1, 3) THEN o_custkey ELSE null END) AS bitmap_union_basic
FROM lineitem
INNER JOIN orders ON lineitem.l_orderkey = orders.o_orderkey AND l_shipdate = o_orderdate
WHERE o_orderdate = '2023-10-18' AND l_suppkey = 3
GROUP BY
l_shipdate,
l_suppkey,
o_orderdate;
```

### Aggregate rewriting
In the definitions of both the query and the materialized view, the aggregated dimensions can either be consistent or inconsistent. 
Filtering of results can be achieved by using fields from the dimensions in the WHERE clause.

The dimensions used in the materialized view need to encompass those used in the query, 
and the metrics utilized in the query can be expressed using the metrics of the materialized view.

**Case 1**

The following case can undergo transparent rewriting. The query and the materialized view use consistent dimensions 
for aggregation, allowing the use of fields from the dimensions to filter results. The query will attempt to use the 
expressions after SELECT in the materialized view.

Materialized view definition:

```sql
CREATE MATERIALIZED VIEW mv4
BUILD IMMEDIATE REFRESH AUTO ON SCHEDULE EVERY 1 hour
DISTRIBUTED BY RANDOM BUCKETS 3
PROPERTIES ('replication_num' = '1')
AS
SELECT
    o_shippriority, o_comment,
    count(distinct CASE WHEN o_shippriority > 1 AND o_orderkey IN (1, 3) THEN o_custkey ELSE null END) AS cnt_1,
    count(distinct CASE WHEN O_SHIPPRIORITY > 2 AND o_orderkey IN (2) THEN o_custkey ELSE null END) AS cnt_2,
    sum(o_totalprice),
    max(o_totalprice),
    min(o_totalprice),
    count(*)
FROM orders
GROUP BY
o_shippriority,
o_comment;
```

Query statement:

```sql
SELECT 
    o_shippriority, o_comment,
    count(distinct CASE WHEN o_shippriority > 1 AND o_orderkey IN (1, 3) THEN o_custkey ELSE null END) AS cnt_1,
    count(distinct CASE WHEN O_SHIPPRIORITY > 2 AND o_orderkey IN (2) THEN o_custkey ELSE null END) AS cnt_2,
    sum(o_totalprice),
    max(o_totalprice),
    min(o_totalprice),
    count(*)
FROM orders
WHERE o_shippriority in (1, 2)
GROUP BY
o_shippriority,
o_comment;
```

**Case 2**

The following query can be transparently rewritten: the query and the materialization use aggregated dimensions 
that are inconsistent, but the dimensions used in the materialized view encompass those used in the query. 
The query can filter results using fields from the dimensions.

The query will attempt to roll up using the functions after SELECT, such as the materialized view's 
bitmap_union will eventually roll up into bitmap_union_count, maintaining consistency with the semantics of 
the count(distinct) in the query.

Materialized view definition:

```sql
CREATE MATERIALIZED VIEW mv5
BUILD IMMEDIATE REFRESH AUTO ON SCHEDULE EVERY 1 hour
DISTRIBUTED BY RANDOM BUCKETS 3
PROPERTIES ('replication_num' = '1')
AS
SELECT
    l_shipdate, o_orderdate, l_partkey, l_suppkey,
    sum(o_totalprice) AS sum_total,
    max(o_totalprice) AS max_total,
    min(o_totalprice) AS min_total,
    count(*) AS count_all,
    bitmap_union(to_bitmap(CASE WHEN o_shippriority > 1 AND o_orderkey IN (1, 3) THEN o_custkey ELSE null END)) AS bitmap_union_basic
FROM lineitem
LEFT OUTER JOIN orders ON lineitem.l_orderkey = orders.o_orderkey AND l_shipdate = o_orderdate
GROUP BY
l_shipdate,
o_orderdate,
l_partkey,
l_suppkey;
```

Query statement:

```sql
SELECT
    l_shipdate, l_suppkey,
    sum(o_totalprice) AS sum_total,
    max(o_totalprice) AS max_total,
    min(o_totalprice) AS min_total,
    count(*) AS count_all,
    count(distinct CASE WHEN o_shippriority > 1 AND o_orderkey IN (1, 3) THEN o_custkey ELSE null END) AS bitmap_union_basic
FROM lineitem
LEFT OUTER JOIN orders ON lineitem.l_orderkey = orders.o_orderkey AND l_shipdate = o_orderdate
WHERE o_orderdate = '2023-10-18' AND l_partkey = 3
GROUP BY
l_shipdate,
l_suppkey;
```

Temporary support for the aggregation roll-up functions is as follows:

| Functions in Queries | Functions in Materialized Views  | Aggregation Functions After Rewriting |
|----------------------|----------------------------------|---------------------------------------|
| max                  | max                              | max                                   |
| min                  | min                              | min                                   |
| sum                  | sum                              | sum                                   |
| count                | count                            | sum                                   |
| count(distinct )     | bitmap_union                     | bitmap_union_count                    |
| bitmap_union         | bitmap_union                     | bitmap_union                          |
| bitmap_union_count   | bitmap_union                     | bitmap_union_count                    |

## Query partial Transparent Rewriting (Coming soon)
When the number of tables in the materialized view is greater than the query, if the materialized view 
satisfies the conditions for JOIN elimination for tables more than the query, transparent rewriting can also occur. 
For example:

**Case 1**

Materialized view definition:

```sql
 CREATE MATERIALIZED VIEW mv6
 BUILD IMMEDIATE REFRESH AUTO ON SCHEDULE EVERY 1 hour
 DISTRIBUTED BY RANDOM BUCKETS 3
 PROPERTIES ('replication_num' = '1')
 AS
 SELECT
     l_linenumber,
     o_custkey,
     ps_availqty
 FROM lineitem
 LEFT OUTER JOIN orders ON L_ORDERKEY = O_ORDERKEY
 LEFT OUTER JOIN partsupp ON l_partkey = ps_partkey
 AND l_suppkey = ps_suppkey;
```

Query statement:
```sql
 SELECT
     l_linenumber,
     o_custkey,
     ps_availqty
 FROM lineitem
 LEFT OUTER JOIN orders ON L_ORDERKEY = O_ORDERKEY;
```

## Union Rewriting (Coming soon)
When the materialized view is not sufficient to provide all the data for the query, it can use Union to return 
data by combining the original table and the materialized view. 
For example:

**Case 1**

Materialized view definition:

```sql
CREATE MATERIALIZED VIEW mv7
BUILD IMMEDIATE REFRESH AUTO ON SCHEDULE EVERY 1 hour
DISTRIBUTED BY RANDOM BUCKETS 3
PROPERTIES ('replication_num' = '1')
AS
SELECT
    o_orderkey,
    o_custkey,
    o_orderstatus,
    o_totalprice
FROM orders
WHERE o_orderkey > 10;
```

Query statement:
```sql
SELECT
    o_orderkey,
    o_custkey,
    o_orderstatus,
    o_totalprice
FROM orders
WHERE o_orderkey > 5;
```

Rewriting result:
```sql
SELECT *
FROM mv
UNION ALL
SELECT
    o_orderkey,
    o_custkey,
    o_orderstatus,
    o_totalprice
FROM orders
WHERE o_orderkey > 5 AND o_orderkey <= 10;
```

## Auxiliary Functions
**Data Consistency Issues After Transparent Rewriting**


The unit of `grace_period` is seconds, referring to the permissible time for inconsistency between the materialized
view and the data in the underlying base tables.

For example, setting `grace_period` to 0 means requiring the materialized view to be consistent with the base 
table data before it can be used for transparent rewriting. As for external tables, 
since changes in data cannot be perceived, the materialized view is used with them.
Regardless of whether the data in the external table is up-to-date or not, this materialized view can be used for 
transparent rewriting. If the external table is configured with an HMS metadata source, 
it becomes capable of perceiving data changes. Configuring the metadata source and enabling data change 
perception functionality will be supported in subsequent iterations.

Setting `grace_period` to 10 means allowing a 10-second delay between the data in the materialized view and 
the data in the base tables. If there is a delay of up to 10 seconds between the data in the materialized 
view and the data in the base tables, the materialized view can still be used for transparent rewriting within 
that time frame.

For internal tables in the materialized view, you can control the maximum delay allowed for the data used by 
the transparent rewriting by setting the `grace_period` property. 
Refer to [CREATE-ASYNC-MATERIALIZED-VIEW](../../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-ASYNC-MATERIALIZED-VIEW.md)

**Viewing and Debugging Transparent Rewrite Hit Information**

You can use the following statements to view the hit information of transparent rewriting for a materialized view.
It will display a concise overview of the transparent rewriting process.

`explain <query_sql>` The information returned is as follows, with the relevant information pertaining to materialized views extracted:
```text
| MaterializedView                                                                                                                                                                                                                                      |
| MaterializedViewRewriteSuccessAndChose:                                                                                                                                                                                                               |
|   Names: mv5                                                                                                                                                                                                                                          |
| MaterializedViewRewriteSuccessButNotChose:                                                                                                                                                                                                            |
|                                                                                                                                                                                                                                                       |
| MaterializedViewRewriteFail:                                                                                                                                                                                                                          |
|   Name: mv4                                                                                                                                                                                                                                           |
|   FailSummary: Match mode is invalid, View struct info is invalid                                                                                                                                                                                     |
|   Name: mv3                                                                                                                                                                                                                                           |
|   FailSummary: Match mode is invalid, Rewrite compensate predicate by view fail, View struct info is invalid                                                                                                                                          |
|   Name: mv1                                                                                                                                                                                                                                           |
|   FailSummary: The columns used by query are not in view, View struct info is invalid                                                                                                                                                                 |
|   Name: mv2                                                                                                                                                                                                                                           |
|   FailSummary: The columns used by query are not in view, View struct info is invalid
```

**MaterializedViewRewriteSuccessAndChose**: Transparent rewrite succeeded, and the materialized view names list
chosen by the CBO.

**MaterializedViewRewriteSuccessButNotChose**: Transparent rewrite succeeded, but the final CBO did not choose the
materialized view names list.

**MaterializedViewRewriteFail**: Lists transparent rewrite failures and summarizes the reasons.



If you want to know the detailed information about materialized view candidates, rewriting, and the final selection process,
you can execute the following statement. It will provide a detailed breakdown of the transparent rewriting process.

`explain memo plan <query_sql>`

## Relevant Environment Variables

| Switch                                                              | Description                                                                                                                       |
|---------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------|
| SET enable_nereids_planner = true;                                  | Asynchronous materialized views are only supported under the new optimizer, so the new optimizer needs to be enabled.             |
| SET enable_materialized_view_rewrite = true;                        | Enable or disable query transparent rewriting, default is disabled                                                                |
| SET materialized_view_rewrite_enable_contain_external_table = true; | Whether materialized views participating in transparent rewriting are allowed to contain external tables, default is not allowed  |
| SET materialized_view_rewrite_success_candidate_num = 3;            | Transparently rewrites the successful result set, allowing the maximum number of CBO candidates to participate, the default is 3  |


## Limitations
- The materialized view definition statement only allows SELECT, FROM, WHERE, JOIN, and GROUP BY clauses. 
The input for JOIN can include simple GROUP BY (aggregation on a single table). 
Supported types of JOIN operations include INNER and LEFT OUTER JOIN. 
Support for other types of JOIN operations will be gradually added.

- Materialized views based on External Tables do not guarantee strong consistency in query results.

- The use of non-deterministic functions to build materialized views is not supported,
including rand, now, current_time, current_date, random, uuid, etc.

- Transparent rewriting does not support window functions.

- There is LIMIT in queries and materialized views, and transparent rewriting is not supported for the time being.
 
- Currently, materialized view definitions cannot utilize views or other materialized views.

- When the query or materialized view has no data, transparent rewriting is not supported.

- Currently, WHERE clause compensation supports scenarios where the materialized view does not have a WHERE clause, 
but the query does, or where the materialized view has a WHERE clause and the query's WHERE clause is a superset 
of the materialized view's. Range condition compensation is not yet supported but will be added gradually.
