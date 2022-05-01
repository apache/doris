---
{
    "title": "物化视图",
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

# 物化视图

物化视图是将预先计算（根据定义好的 SELECT 语句）好的数据集，存储在 Doris 中的一个特殊的表。

物化视图的出现主要是为了满足用户，既能对原始明细数据的任意维度分析，也能快速的对固定维度进行分析查询。

## 适用场景

- 分析需求覆盖明细数据查询以及固定维度查询两方面。
- 查询仅涉及表中的很小一部分列或行。
- 查询包含一些耗时处理操作，比如：时间很久的聚合操作等。
- 查询需要匹配不同前缀索引。

## 优势

- 对于那些经常重复的使用相同的子查询结果的查询性能大幅提升。
- Doris自动维护物化视图的数据，无论是新的导入，还是删除操作都能保证base 表和物化视图表的数据一致性。无需任何额外的人工维护成本。
- 查询时，会自动匹配到最优物化视图，并直接从物化视图中读取数据。

*自动维护物化视图的数据会造成一些维护开销，会在后面的物化视图的局限性中展开说明。*

## 物化视图 VS Rollup

在没有物化视图功能之前，用户一般都是使用 Rollup 功能通过预聚合方式提升查询效率的。但是 Rollup 具有一定的局限性，他不能基于明细模型做预聚合。

物化视图则在覆盖了 Rollup 的功能的同时，还能支持更丰富的聚合函数。所以物化视图其实是 Rollup 的一个超集。

也就是说，之前 [ALTER TABLE ADD ROLLUP](../sql-manual/sql-reference/Data-Definition-Statements/Alter/ALTER-TABLE-ROLLUP.md) 语法支持的功能现在均可以通过 [CREATE MATERIALIZED VIEW](../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-MATERIALIZED-VIEW.md) 实现。

## 使用物化视图

Doris 系统提供了一整套对物化视图的 DDL 语法，包括创建，查看，删除。DDL 的语法和 PostgreSQL, Oracle都是一致的。

### 创建物化视图

这里首先你要根据你的查询语句的特点来决定创建一个什么样的物化视图。这里并不是说你的物化视图定义和你的某个查询语句一模一样就最好。这里有两个原则：

1. 从查询语句中**抽象**出，多个查询共有的分组和聚合方式作为物化视图的定义。
2. 不需要给所有维度组合都创建物化视图。

首先第一个点，一个物化视图如果抽象出来，并且多个查询都可以匹配到这张物化视图。这种物化视图效果最好。因为物化视图的维护本身也需要消耗资源。

如果物化视图只和某个特殊的查询很贴合，而其他查询均用不到这个物化视图。则会导致这张物化视图的性价比不高，既占用了集群的存储资源，还不能为更多的查询服务。

所以用户需要结合自己的查询语句，以及数据维度信息去抽象出一些物化视图的定义。

第二点就是，在实际的分析查询中，并不会覆盖到所有的维度分析。所以给常用的维度组合创建物化视图即可，从而到达一个空间和时间上的平衡。

创建物化视图是一个异步的操作，也就是说用户成功提交创建任务后，Doris 会在后台对存量的数据进行计算，直到创建成功。

具体的语法可查看[CREATE MATERIALIZED VIEW](../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-MATERIALIZED-VIEW.html) 。

### 支持聚合函数

目前物化视图创建语句支持的聚合函数有：

- SUM, MIN, MAX (Version 0.12)
- COUNT, BITMAP_UNION, HLL_UNION (Version 0.13)
- BITMAP_UNION 的形式必须为：`BITMAP_UNION(TO_BITMAP(COLUMN))` column 列的类型只能是整数（largeint也不支持), 或者 `BITMAP_UNION(COLUMN)` 且 base 表为 AGG 模型。
- HLL_UNION 的形式必须为：`HLL_UNION(HLL_HASH(COLUMN))` column 列的类型不能是 DECIMAL , 或者 `HLL_UNION(COLUMN)` 且 base 表为 AGG 模型。

### 更新策略

为保证物化视图表和 Base 表的数据一致性, Doris 会将导入，删除等对 base 表的操作都同步到物化视图表中。并且通过增量更新的方式来提升更新效率。通过事务方式来保证原子性。

比如如果用户通过 INSERT 命令插入数据到 base 表中，则这条数据会同步插入到物化视图中。当 base 表和物化视图表均写入成功后，INSERT 命令才会成功返回。

### 查询自动匹配

物化视图创建成功后，用户的查询不需要发生任何改变，也就是还是查询的 base 表。Doris 会根据当前查询的语句去自动选择一个最优的物化视图，从物化视图中读取数据并计算。

用户可以通过 EXPLAIN 命令来检查当前查询是否使用了物化视图。

物化视图中的聚合和查询中聚合的匹配关系：

| 物化视图聚合 | 查询中聚合                                             |
| ------------ | ------------------------------------------------------ |
| sum          | sum                                                    |
| min          | min                                                    |
| max          | max                                                    |
| count        | count                                                  |
| bitmap_union | bitmap_union, bitmap_union_count, count(distinct)      |
| hll_union    | hll_raw_agg, hll_union_agg, ndv, approx_count_distinct |

其中 bitmap 和 hll 的聚合函数在查询匹配到物化视图后，查询的聚合算子会根据物化视图的表结构进行一个改写。详细见实例2。

### 查询物化视图

查看当前表都有哪些物化视图，以及他们的表结构都是什么样的。通过下面命令：

```sql
MySQL [test]> desc mv_test all;
+-----------+---------------+-----------------+----------+------+-------+---------+--------------+
| IndexName | IndexKeysType | Field           | Type     | Null | Key   | Default | Extra        |
+-----------+---------------+-----------------+----------+------+-------+---------+--------------+
| mv_test   | DUP_KEYS      | k1              | INT      | Yes  | true  | NULL    |              |
|           |               | k2              | BIGINT   | Yes  | true  | NULL    |              |
|           |               | k3              | LARGEINT | Yes  | true  | NULL    |              |
|           |               | k4              | SMALLINT | Yes  | false | NULL    | NONE         |
|           |               |                 |          |      |       |         |              |
| mv_2      | AGG_KEYS      | k2              | BIGINT   | Yes  | true  | NULL    |              |
|           |               | k4              | SMALLINT | Yes  | false | NULL    | MIN          |
|           |               | k1              | INT      | Yes  | false | NULL    | MAX          |
|           |               |                 |          |      |       |         |              |
| mv_3      | AGG_KEYS      | k1              | INT      | Yes  | true  | NULL    |              |
|           |               | to_bitmap(`k2`) | BITMAP   | No   | false |         | BITMAP_UNION |
|           |               |                 |          |      |       |         |              |
| mv_1      | AGG_KEYS      | k4              | SMALLINT | Yes  | true  | NULL    |              |
|           |               | k1              | BIGINT   | Yes  | false | NULL    | SUM          |
|           |               | k3              | LARGEINT | Yes  | false | NULL    | SUM          |
|           |               | k2              | BIGINT   | Yes  | false | NULL    | MIN          |
+-----------+---------------+-----------------+----------+------+-------+---------+--------------+
```

可以看到当前 `mv_test` 表一共有三张物化视图：mv_1, mv_2 和 mv_3，以及他们的表结构。

### 删除物化视图

如果用户不再需要物化视图，则可以通过命令删除物化视图。

具体的语法可查看[DROP MATERIALIZED VIEW](../sql-manual/sql-reference/Data-Definition-Statements/Drop/DROP-MATERIALIZED-VIEW.html) 



## 最佳实践1

使用物化视图一般分为以下几个步骤：

1. 创建物化视图
2. 异步检查物化视图是否构建完成
3. 查询并自动匹配物化视图

**首先是第一步：创建物化视图**

假设用户有一张销售记录明细表，存储了每个交易的交易id，销售员，售卖门店，销售时间，以及金额。建表语句为：

假设用户有一张销售记录明细表，存储了每个交易的交易id，销售员，售卖门店，销售时间，以及金额。建表语句为：

```sql
create table sales_records(record_id int, seller_id int, store_id int, sale_date date, sale_amt bigint) distributed by hash(record_id) properties("replication_num" = "1");
```

这张 `sales_records` 的表结构如下：

```sql
MySQL [test]> desc sales_records;
+-----------+--------+------+-------+---------+-------+
| Field     | Type   | Null | Key   | Default | Extra |
+-----------+--------+------+-------+---------+-------+
| record_id | INT    | Yes  | true  | NULL    |       |
| seller_id | INT    | Yes  | true  | NULL    |       |
| store_id  | INT    | Yes  | true  | NULL    |       |
| sale_date | DATE   | Yes  | false | NULL    | NONE  |
| sale_amt  | BIGINT | Yes  | false | NULL    | NONE  |
+-----------+--------+------+-------+---------+-------+
```

这时候如果用户经常对不同门店的销售量进行一个分析查询，则可以给这个 `sales_records` 表创建一张以售卖门店分组，对相同售卖门店的销售额求和的一个物化视图。创建语句如下：

```sql
MySQL [test]> create materialized view store_amt as select store_id, sum(sale_amt) from sales_records group by store_id;
```

后端返回下图，则说明创建物化视图任务提交成功。

```sql
Query OK, 0 rows affected (0.012 sec)
```

**第二步：检查物化视图是否构建完成**

由于创建物化视图是一个异步的操作，用户在提交完创建物化视图任务后，需要异步的通过命令检查物化视图是否构建完成。命令如下：

```sql
SHOW ALTER TABLE ROLLUP FROM db_name; (Version 0.12)
SHOW ALTER TABLE MATERIALIZED VIEW FROM db_name; (Version 0.13)
```

这个命令中 `db_name` 是一个参数, 你需要替换成自己真实的 db 名称。命令的结果是显示这个 db 的所有创建物化视图的任务。结果如下：

```sql
+-------+---------------+---------------------+---------------------+---------------+-----------------+----------+---------------+-----------+-------------------------------------------------------------------------------------------------------------------------+----------+---------+
| JobId | TableName     | CreateTime          | FinishedTime        | BaseIndexName | RollupIndexName | RollupId | TransactionId | State     | Msg                                                                                                                     | Progress | Timeout |
+-------+---------------+---------------------+---------------------+---------------+-----------------+----------+---------------+-----------+-------------------------------------------------------------------------------------------------------------------------+----------+---------+
| 22036 | sales_records | 2020-07-30 20:04:28 | 2020-07-30 20:04:57 | sales_records | store_amt       | 22037    | 5008          | FINISHED  |                                                                                                                         | NULL     | 86400   |
+-------+---------------+---------------------+---------------------+---------------+-----------------+----------+---------------+-----------+-------------------------------------------------------------------------------------------------------------------------+----------+---------+
```

其中 TableName 指的是物化视图的数据来自于哪个表，RollupIndexName 指的是物化视图的名称叫什么。其中比较重要的指标是 State。

当创建物化视图任务的 State 已经变成 FINISHED 后，就说明这个物化视图已经创建成功了。这就意味着，查询的时候有可能自动匹配到这张物化视图了。

**第三步：查询**

当创建完成物化视图后，用户再查询不同门店的销售量时，就会直接从刚才创建的物化视图 `store_amt` 中读取聚合好的数据。达到提升查询效率的效果。

用户的查询依旧指定查询 `sales_records` 表，比如：

```sql
SELECT store_id, sum(sale_amt) FROM sales_records GROUP BY store_id;
```

上面查询就能自动匹配到 `store_amt`。用户可以通过下面命令，检验当前查询是否匹配到了合适的物化视图。

```sql
EXPLAIN SELECT store_id, sum(sale_amt) FROM sales_records GROUP BY store_id;
+-----------------------------------------------------------------------------+
| Explain String                                                              |
+-----------------------------------------------------------------------------+
| PLAN FRAGMENT 0                                                             |
|  OUTPUT EXPRS:<slot 2> `store_id` | <slot 3> sum(`sale_amt`)                |
|   PARTITION: UNPARTITIONED                                                  |
|                                                                             |
|   RESULT SINK                                                               |
|                                                                             |
|   4:EXCHANGE                                                                |
|                                                                             |
| PLAN FRAGMENT 1                                                             |
|  OUTPUT EXPRS:                                                              |
|   PARTITION: HASH_PARTITIONED: <slot 2> `store_id`                          |
|                                                                             |
|   STREAM DATA SINK                                                          |
|     EXCHANGE ID: 04                                                         |
|     UNPARTITIONED                                                           |
|                                                                             |
|   3:AGGREGATE (merge finalize)                                              |
|   |  output: sum(<slot 3> sum(`sale_amt`))                                  |
|   |  group by: <slot 2> `store_id`                                          |
|   |                                                                         |
|   2:EXCHANGE                                                                |
|                                                                             |
| PLAN FRAGMENT 2                                                             |
|  OUTPUT EXPRS:                                                              |
|   PARTITION: RANDOM                                                         |
|                                                                             |
|   STREAM DATA SINK                                                          |
|     EXCHANGE ID: 02                                                         |
|     HASH_PARTITIONED: <slot 2> `store_id`                                   |
|                                                                             |
|   1:AGGREGATE (update serialize)                                            |
|   |  STREAMING                                                              |
|   |  output: sum(`sale_amt`)                                                |
|   |  group by: `store_id`                                                   |
|   |                                                                         |
|   0:OlapScanNode                                                            |
|      TABLE: sales_records                                                   |
|      PREAGGREGATION: ON                                                     |
|      partitions=1/1                                                         |
|      rollup: store_amt                                                      |
|      tabletRatio=10/10                                                      |
|      tabletList=22038,22040,22042,22044,22046,22048,22050,22052,22054,22056 |
|      cardinality=0                                                          |
|      avgRowSize=0.0                                                         |
|      numNodes=1                                                             |
+-----------------------------------------------------------------------------+
45 rows in set (0.006 sec)
```

其中最重要的就是 OlapScanNode 中的 rollup 属性。可以看到当前查询的 rollup 显示的是 `store_amt`。也就是说查询已经正确匹配到物化视图 `store_amt`, 并直接从物化视图中读取数据了。

## 最佳实践2 PV,UV

业务场景: 计算广告的 UV，PV

假设用户的原始广告点击数据存储在 Doris，那么针对广告 PV, UV 查询就可以通过创建 `bitmap_union` 的物化视图来提升查询速度。

通过下面语句首先创建一个存储广告点击数据明细的表，包含每条点击的点击事件，点击的是什么广告，通过什么渠道点击，以及点击的用户是谁。

```sql
MySQL [test]> create table advertiser_view_record(time date, advertiser varchar(10), channel varchar(10), user_id int) distributed by hash(time) properties("replication_num" = "1");
Query OK, 0 rows affected (0.014 sec)
```

原始的广告点击数据表结构为：

```sql
MySQL [test]> desc advertiser_view_record;
+------------+-------------+------+-------+---------+-------+
| Field      | Type        | Null | Key   | Default | Extra |
+------------+-------------+------+-------+---------+-------+
| time       | DATE        | Yes  | true  | NULL    |       |
| advertiser | VARCHAR(10) | Yes  | true  | NULL    |       |
| channel    | VARCHAR(10) | Yes  | false | NULL    | NONE  |
| user_id    | INT         | Yes  | false | NULL    | NONE  |
+------------+-------------+------+-------+---------+-------+
4 rows in set (0.001 sec)
```

1. 创建物化视图

   由于用户想要查询的是广告的 UV 值，也就是需要对相同广告的用户进行一个精确去重，则查询一般为：

   ```sql
   SELECT advertiser, channel, count(distinct user_id) FROM advertiser_view_record GROUP BY advertiser, channel;
   ```

   针对这种求 UV 的场景，我们就可以创建一个带 `bitmap_union` 的物化视图从而达到一个预先精确去重的效果。

   在 Doris 中，`count(distinct)` 聚合的结果和 `bitmap_union_count`聚合的结果是完全一致的。而`bitmap_union_count` 等于 `bitmap_union` 的结果求 count， 所以如果查询中**涉及到 `count(distinct)` 则通过创建带 `bitmap_union` 聚合的物化视图方可加快查询**。

   针对这个 case，则可以创建一个根据广告和渠道分组，对 `user_id` 进行精确去重的物化视图。

   ```sql
   MySQL [test]> create materialized view advertiser_uv as select advertiser, channel, bitmap_union(to_bitmap(user_id)) from advertiser_view_record group by advertiser, channel;
   Query OK, 0 rows affected (0.012 sec)
   ```

   *注意：因为本身 user_id 是一个 INT 类型，所以在 Doris 中需要先将字段通过函数 `to_bitmap` 转换为 bitmap 类型然后才可以进行 `bitmap_union` 聚合。*

   创建完成后, 广告点击明细表和物化视图表的表结构如下：

   ```sql
   MySQL [test]> desc advertiser_view_record all;
   +------------------------+---------------+----------------------+-------------+------+-------+---------+--------------+
   | IndexName              | IndexKeysType | Field                | Type        | Null | Key   | Default | Extra        |
   +------------------------+---------------+----------------------+-------------+------+-------+---------+--------------+
   | advertiser_view_record | DUP_KEYS      | time                 | DATE        | Yes  | true  | NULL    |              |
   |                        |               | advertiser           | VARCHAR(10) | Yes  | true  | NULL    |              |
   |                        |               | channel              | VARCHAR(10) | Yes  | false | NULL    | NONE         |
   |                        |               | user_id              | INT         | Yes  | false | NULL    | NONE         |
   |                        |               |                      |             |      |       |         |              |
   | advertiser_uv          | AGG_KEYS      | advertiser           | VARCHAR(10) | Yes  | true  | NULL    |              |
   |                        |               | channel              | VARCHAR(10) | Yes  | true  | NULL    |              |
   |                        |               | to_bitmap(`user_id`) | BITMAP      | No   | false |         | BITMAP_UNION |
   +------------------------+---------------+----------------------+-------------+------+-------+---------+--------------+
   ```

2. 查询自动匹配

   当物化视图表创建完成后，查询广告 UV 时，Doris就会自动从刚才创建好的物化视图 `advertiser_uv` 中查询数据。比如原始的查询语句如下：

   ```sql
   SELECT advertiser, channel, count(distinct user_id) FROM advertiser_view_record GROUP BY advertiser, channel;
   ```

   在选中物化视图后，实际的查询会转化为：

   ```sql
   SELECT advertiser, channel, bitmap_union_count(to_bitmap(user_id)) FROM advertiser_uv GROUP BY advertiser, channel;
   ```

   通过 EXPLAIN 命令可以检验到 Doris 是否匹配到了物化视图：

   ```sql
   MySQL [test]> explain SELECT advertiser, channel, count(distinct user_id) FROM  advertiser_view_record GROUP BY advertiser, channel;
   +-------------------------------------------------------------------------------------------------------------------------------------------------------------------+
   | Explain String                                                                                                                                                    |
   +-------------------------------------------------------------------------------------------------------------------------------------------------------------------+
   | PLAN FRAGMENT 0                                                                                                                                                   |
   |  OUTPUT EXPRS:<slot 7> `advertiser` | <slot 8> `channel` | <slot 9> bitmap_union_count(`default_cluster:test`.`advertiser_view_record`.`mv_bitmap_union_user_id`) |
   |   PARTITION: UNPARTITIONED                                                                                                                                        |
   |                                                                                                                                                                   |
   |   RESULT SINK                                                                                                                                                     |
   |                                                                                                                                                                   |
   |   4:EXCHANGE                                                                                                                                                      |
   |                                                                                                                                                                   |
   | PLAN FRAGMENT 1                                                                                                                                                   |
   |  OUTPUT EXPRS:                                                                                                                                                    |
   |   PARTITION: HASH_PARTITIONED: <slot 4> `advertiser`, <slot 5> `channel`                                                                                          |
   |                                                                                                                                                                   |
   |   STREAM DATA SINK                                                                                                                                                |
   |     EXCHANGE ID: 04                                                                                                                                               |
   |     UNPARTITIONED                                                                                                                                                 |
   |                                                                                                                                                                   |
   |   3:AGGREGATE (merge finalize)                                                                                                                                    |
   |   |  output: bitmap_union_count(<slot 6> bitmap_union_count(`default_cluster:test`.`advertiser_view_record`.`mv_bitmap_union_user_id`))                           |
   |   |  group by: <slot 4> `advertiser`, <slot 5> `channel`                                                                                                          |
   |   |                                                                                                                                                               |
   |   2:EXCHANGE                                                                                                                                                      |
   |                                                                                                                                                                   |
   | PLAN FRAGMENT 2                                                                                                                                                   |
   |  OUTPUT EXPRS:                                                                                                                                                    |
   |   PARTITION: RANDOM                                                                                                                                               |
   |                                                                                                                                                                   |
   |   STREAM DATA SINK                                                                                                                                                |
   |     EXCHANGE ID: 02                                                                                                                                               |
   |     HASH_PARTITIONED: <slot 4> `advertiser`, <slot 5> `channel`                                                                                                   |
   |                                                                                                                                                                   |
   |   1:AGGREGATE (update serialize)                                                                                                                                  |
   |   |  STREAMING                                                                                                                                                    |
   |   |  output: bitmap_union_count(`default_cluster:test`.`advertiser_view_record`.`mv_bitmap_union_user_id`)                                                        |
   |   |  group by: `advertiser`, `channel`                                                                                                                            |
   |   |                                                                                                                                                               |
   |   0:OlapScanNode                                                                                                                                                  |
   |      TABLE: advertiser_view_record                                                                                                                                |
   |      PREAGGREGATION: ON                                                                                                                                           |
   |      partitions=1/1                                                                                                                                               |
   |      rollup: advertiser_uv                                                                                                                                        |
   |      tabletRatio=10/10                                                                                                                                            |
   |      tabletList=22084,22086,22088,22090,22092,22094,22096,22098,22100,22102                                                                                       |
   |      cardinality=0                                                                                                                                                |
   |      avgRowSize=0.0                                                                                                                                               |
   |      numNodes=1                                                                                                                                                   |
   +-------------------------------------------------------------------------------------------------------------------------------------------------------------------+
   45 rows in set (0.030 sec)
   ```

   在 EXPLAIN 的结果中，首先可以看到 OlapScanNode 的 rollup 属性值为 advertiser_uv。也就是说，查询会直接扫描物化视图的数据。说明匹配成功。

   其次对于 `user_id` 字段求 `count(distinct)` 被改写为求 `bitmap_union_count(to_bitmap)`。也就是通过 bitmap 的方式来达到精确去重的效果。

   ## 最佳实践3

   业务场景：匹配更丰富的前缀索引

   用户的原始表有 （k1, k2, k3） 三列。其中 k1, k2 为前缀索引列。这时候如果用户查询条件中包含 `where k1=1 and k2=2` 就能通过索引加速查询。

   但是有些情况下，用户的过滤条件无法匹配到前缀索引，比如 `where k3=3`。则无法通过索引提升查询速度。

   创建以 k3 作为第一列的物化视图就可以解决这个问题。

   1. 创建物化视图

      ```sql
      CREATE MATERIALIZED VIEW mv_1 as SELECT k3, k2, k1 FROM tableA ORDER BY k3;
      ```

      通过上面语法创建完成后，物化视图中既保留了完整的明细数据，且物化视图的前缀索引为 k3 列。表结构如下：

      ```sql
      MySQL [test]> desc tableA all;
      +-----------+---------------+-------+------+------+-------+---------+-------+
      | IndexName | IndexKeysType | Field | Type | Null | Key   | Default | Extra |
      +-----------+---------------+-------+------+------+-------+---------+-------+
      | tableA    | DUP_KEYS      | k1    | INT  | Yes  | true  | NULL    |       |
      |           |               | k2    | INT  | Yes  | true  | NULL    |       |
      |           |               | k3    | INT  | Yes  | true  | NULL    |       |
      |           |               |       |      |      |       |         |       |
      | mv_1      | DUP_KEYS      | k3    | INT  | Yes  | true  | NULL    |       |
      |           |               | k2    | INT  | Yes  | false | NULL    | NONE  |
      |           |               | k1    | INT  | Yes  | false | NULL    | NONE  |
      +-----------+---------------+-------+------+------+-------+---------+-------+
      ```

   2. 查询匹配

      这时候如果用户的查询存在 k3 列的过滤条件是，比如：

      ```sql
      select k1, k2, k3 from table A where k3=3;
      ```

      这时候查询就会直接从刚才创建的 mv_1 物化视图中读取数据。物化视图对 k3 是存在前缀索引的，查询效率也会提升。

   ## 局限性

   1. 物化视图的聚合函数的参数不支持表达式仅支持单列，比如： sum(a+b)不支持。
   2. 如果删除语句的条件列，在物化视图中不存在，则不能进行删除操作。如果一定要删除数据，则需要先将物化视图删除，然后方可删除数据。
   3. 单表上过多的物化视图会影响导入的效率：导入数据时，物化视图和 base 表数据是同步更新的，如果一张表的物化视图表超过10张，则有可能导致导入速度很慢。这就像单次导入需要同时导入10张表数据是一样的。
   4. 相同列，不同聚合函数，不能同时出现在一张物化视图中，比如：select sum(a), min(a) from table 不支持。
   5. 物化视图针对 Unique Key数据模型，只能改变列顺序，不能起到聚合的作用，所以在Unique Key模型上不能通过创建物化视图的方式对数据进行粗粒度聚合操作

   ## 异常错误

   1. DATA_QUALITY_ERR: "The data quality does not satisfy, please check your data" 由于数据质量问题导致物化视图创建失败。 注意：bitmap类型仅支持正整型, 如果原始数据中存在负数，会导致物化视图创建失败



## 更多帮助

关于物化视图使用的更多详细语法及最佳实践，请参阅 [CREATE MATERIALIZED VIEW](../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-MATERIALIZED-VIEW.md) 和 [DROP MATERIALIZED VIEW](../sql-manual/sql-reference/Data-Definition-Statements/Drop/DROP-MATERIALIZED-VIEW.html) 命令手册，你也可以在 MySql 客户端命令行下输入 `HELP CREATE MATERIALIZED VIEW` 和`HELP DROP MATERIALIZED VIEW`  获取更多帮助信息。
