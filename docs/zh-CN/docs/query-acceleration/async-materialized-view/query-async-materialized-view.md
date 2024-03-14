---
{
    "title": "查询异步物化视图",
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

## 概述
Doris 的异步物化视图采用了基于 SPJG（SELECT-PROJECT-JOIN-GROUP-BY）模式结构信息来进行透明改写的算法。

Doris 可以分析查询 SQL 的结构信息，自动寻找满足要求的物化视图，并尝试进行透明改写，使用最优的物化视图来表达查询SQL。

通过使用预计算的物化视图结果，可以大幅提高查询性能，减少计算成本。

以 TPC-H 的三张 lineitem，orders 和 partsupp 表来描述直接查询物化视图和使用物化视图进行查询透明改写的能力。
表的定义如下：
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

## 直查物化视图
物化视图可以看作是表，可以像正常的表一样直接查询。

**用例1:**

物化视图的定义语法，详情见 [CREATE-ASYNC-MATERIALIZED-VIEW](../../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-ASYNC-MATERIALIZED-VIEW.md)

mv 定义:
```sql
CREATE MATERIALIZED VIEW mv1
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
查询语句:

可以对物化视图添加过滤条件和聚合等，进行直接查询。

```sql
SELECT l_linenumber,
       o_custkey
FROM mv1
WHERE l_linenumber > 1 and o_orderdate = '2023-10-18';
```

## 透明改写能力
### JOIN 改写
Join 改写指的是查询和物化使用的表相同，可以在物化视图和查询 Join 的输入或者 Join 的外层写 where，优化器对此 pattern 的查询会尝试进行透明改写。

支持多表 Join，支持 Join 的类型为 inner，left。其他类型在不断拓展中。

**用例1:**

如下查询可进行透明改写，条件 `l_linenumber > 1`可以上拉，从而进行透明改写，使用物化视图的预计算结果来表达查询。

mv 定义:
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
查询语句:
```sql
SELECT l_linenumber,
       o_custkey
FROM lineitem
LEFT OUTER JOIN orders
ON l_orderkey = o_orderkey
WHERE l_linenumber > 1 and o_orderdate = '2023-10-18';
```

**用例2:**

JOIN衍生，当查询和物化视图的 JOIN 的类型不一致时，如果物化可以提供查询所需的所有数据时，通过在 JOIN 的外部补偿谓词，也可以进行透明改写，

举例如下

mv 定义:
```sql
CREATE MATERIALIZED VIEW mv3
BUILD IMMEDIATE REFRESH AUTO ON SCHEDULE EVERY 1 hour
DISTRIBUTED BY RANDOM BUCKETS 3
PROPERTIES ('replication_num' = '1')
AS
SELECT
    l_shipdate, l_suppkey, o_orderdate,
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

查询语句:
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

### 聚合改写
查询和物化视图定义中，聚合的维度可以一致或者不一致，可以使用维度中的字段写 WHERE 对结果进行过滤。

物化视图使用的维度需要包含查询的维度，并且查询使用的指标可以使用物化视图的指标来表示。

**用例1**

如下查询可以进行透明改写，查询和物化使用聚合的维度一致，可以使用维度中的字段进行过滤结果，并且查询会尝试使用物化视图 SELECT 后的表达式。

mv 定义:
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

查询语句:

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

**用例2**

如下查询可以进行透明改写，查询和物化使用聚合的维度不一致，物化视图使用的维度包含查询的维度。 查询可以使用维度中的字段对结果进行过滤，

查询会尝试使用物化视图 SELECT 后的函数进行上卷，如物化视图的 `bitmap_union` 最后会上卷成 `bitmap_union_count`，和查询中
`count(distinct)` 的语义保持一致。

mv 定义:
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

查询语句:
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

暂时目前支持的聚合上卷函数列表如下：

| 查询中函数              | 物化视图中函数       | 函数上卷后               |
|--------------------|---------------|---------------------|
| max                | max           | max                 |
| min                | min           | min                 |
| sum                | sum           | sum                 |
| count              | count         | sum                 |
| count(distinct )   | bitmap_union  | bitmap_union_count  |
| bitmap_union       | bitmap_union  | bitmap_union        |
| bitmap_union_count | bitmap_union  | bitmap_union_count  |

## Query partial 透明改写（Coming soon）
当物化视图的表比查询多时，如果物化视图比查询多的表满足 JOIN 消除的条件，那么也可以进行透明改写，如下可以进行透明改写，待支持。

**用例1**

mv 定义:
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

查询语句：
```sql
 SELECT
     l_linenumber,
     o_custkey,
     ps_availqty
 FROM lineitem
 LEFT OUTER JOIN orders ON L_ORDERKEY = O_ORDERKEY;
```

## Union 改写（Coming soon）
当物化视图不足以提供查询的所有数据时，可以通过 Union 的方式，将查询原表和物化视图 Union 起来返回数据，如下可以进行透明改写，待支持。

**用例1**

mv 定义:
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

查询语句：
```sql
SELECT
    o_orderkey,
    o_custkey,
    o_orderstatus,
    o_totalprice
FROM orders
WHERE o_orderkey > 5;
```

改写结果示意：
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

## 辅助功能
**透明改写后数据一致性问题**

`grace_period` 的单位是秒，指的是容许物化视图和所用基表数据不一致的时间。
比如 `grace_period` 设置成0，意味要求物化视图和基表数据保持一致，此物化视图才可用于透明改写；对于外表，因为无法感知数据变更，所以物化视图使用了外表，

无论外表的数据是不是最新的，都可以使用此物化视图用于透明改写，如果外表配置了 HMS 元数据源，是可以感知数据变更的，配置数据源和感知数据变更的功能会在后面迭代支持。

如果设置成10，意味物化视图和基表数据允许10s的延迟，如果物化视图的数据和基表的数据有延迟，如果在10s内，此物化视图都可以用于透明改写。

对于物化视图中的内表，可以通过设定 `grace_period` 属性来控制透明改写使用的物化视图所允许数据最大的延迟时间。
可查看 [CREATE-ASYNC-MATERIALIZED-VIEW](../../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-ASYNC-MATERIALIZED-VIEW.md)

**查询透明改写命中情况查看和调试**

可通过如下语句查看物化视图的透明改写命中情况，会展示查询透明改写简要过程信息。

`explain <query_sql>` 返回的信息如下，截取了物化视图相关的信息
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
**MaterializedViewRewriteSuccessAndChose**：透明改写成功，并且CBO选择的物化视图名称列表。

**MaterializedViewRewriteSuccessButNotChose**：透明改写成功，但是最终CBO没有选择的物化视图名称列表。

**MaterializedViewRewriteFail**：列举透明改写失败及原因摘要。


如果想知道物化视图候选，改写和最终选择情况的过程详细信息，可以执行如下语句，会展示透明改写过程详细的信息。

`explain memo plan <query_sql>`

## 相关环境变量

| 开关                                                                  | 说明                                |
|---------------------------------------------------------------------|-----------------------------------|
| SET enable_nereids_planner = true;                                  | 异步物化视图只有在新优化器下才支持，所以需要开启新优化器      |
| SET enable_materialized_view_rewrite = true;                        | 开启或者关闭查询透明改写，默认关闭                 |
| SET materialized_view_rewrite_enable_contain_external_table = true; | 参与透明改写的物化视图是否允许包含外表，默认不允许         |
| SET materialized_view_rewrite_success_candidate_num = 3;            | 透明改写成功的结果集合，允许参与到CBO候选的最大数量，默认是3个 |


## 限制
- 物化视图定义语句中只允许包含 SELECT、FROM、WHERE、JOIN、GROUP BY 语句，JOIN 的输入可以包含简单的 GROUP BY（单表聚合），其中JOIN的支持的类型为
INNER 和 LEFT OUTER JOIN 其他类型的 JOIN 操作逐步支持。
- 基于 External Table 的物化视图不保证查询结果强一致。
- 不支持使用非确定性函数来构建物化视图，包括 rand、now、current_time、current_date、random、uuid等。
- 不支持窗口函数的透明改写。
- 查询和物化视图中有 LIMIT，暂时不支持透明改写。
- 物化视图的定义暂时不能使用视图和物化视图。
- 当查询或者物化视图没有数据时，不支持透明改写。
- 目前 WHERE 条件补偿，支持物化视图没有 WHERE，查询有 WHERE情况的条件补偿；或者物化视图有 WHERE 且查询的 WHERE 条件是物化视图的超集。
目前暂时还不支持范围的条件补偿，比如物化视图定义是 a > 5，查询是 a > 10，逐步支持。