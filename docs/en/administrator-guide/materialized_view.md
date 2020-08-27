---
{
    "title": "Materialized view",
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

# Materialized view
A materialized view is a data set that is pre-calculated (according to a defined SELECT statement) and stored in a special table in Doris.

The emergence of materialized views is mainly to satisfy users. It can analyze any dimension of the original detailed data, but also can quickly analyze and query fixed dimensions.

## When to use materialized view

+ Analyze requirements to cover both detailed data query and fixed-dimensional query.
+ The query only involves a small part of the columns or rows in the table.
+ The query contains some time-consuming processing operations, such as long-time aggregation operations.
+ The query needs to match different prefix indexes.

## Advantage

+ For those queries that frequently use the same sub-query results repeatedly, the performance is greatly improved
+ Doris automatically maintains the data of the materialized view, whether it is a new import or delete operation, it can ensure the data consistency of the base table and the materialized view table. No need for any additional labor maintenance costs.
+ When querying, it will automatically match the optimal materialized view and read data directly from the materialized view.

*Automatic maintenance of materialized view data will cause some maintenance overhead, which will be explained in the limitations of materialized views later.*

## Materialized View VS Rollup

Before the materialized view function, users generally used the Rollup function to improve query efficiency through pre-aggregation. However, Rollup has certain limitations. It cannot do pre-aggregation based on the detailed model.

Materialized views cover the functions of Rollup while also supporting richer aggregate functions. So the materialized view is actually a superset of Rollup.

In other words, the functions previously supported by the `ALTER TABLE ADD ROLLUP` syntax can now be implemented by `CREATE MATERIALIZED VIEW`.

## Use materialized views

The Doris system provides a complete set of DDL syntax for materialized views, including creating, viewing, and deleting. The syntax of DDL is consistent with PostgreSQL and Oracle.

### Create a materialized view

Here you must first decide what kind of materialized view to create based on the characteristics of your query statement. This is not to say that your materialized view definition is exactly the same as one of your query statements. There are two principles here:

1. **Abstract** from the query statement, the grouping and aggregation methods shared by multiple queries are used as the definition of the materialized view.
2. It is not necessary to create materialized views for all dimension combinations.

First of all, the first point, if a materialized view is abstracted, and multiple queries can be matched to this materialized view. This materialized view works best. Because the maintenance of the materialized view itself also consumes resources.

If the materialized view only fits a particular query, and other queries do not use this materialized view. As a result, the materialized view is not cost-effective, which not only occupies the storage resources of the cluster, but cannot serve more queries.

Therefore, users need to combine their own query statements and data dimension information to abstract the definition of some materialized views.

The second point is that in the actual analysis query, not all dimensional analysis will be covered. Therefore, it is enough to create a materialized view for the commonly used combination of dimensions, so as to achieve a space and time balance.

The materialized view can be created by the following command. Creating a materialized view is an asynchronous operation, which means that after the user successfully submits the creation task, Doris will calculate the existing data in the background until the creation is successful.

```
CREATE MATERIALIZED VIEW
```

The specific syntax can be viewed through the following command:

```
HELP CREATE MATERIALIZED VIEW
```

### Support aggregate functions

The aggregate functions currently supported by the materialized view function are:

+ SUM, MIN, MAX (Version 0.12)
+ COUNT, BITMAP\_UNION, HLL\_UNION (Version 0.13)

+ The form of BITMAP\_UNION must be: `BITMAP_UNION(TO_BITMAP(COLUMN))` The column type can only be an integer (largeint also does not support), or `BITMAP_UNION(COLUMN)` and the base table is an AGG model.
+ The form of HLL\_UNION must be: `HLL_UNION(HLL_HASH(COLUMN))` The column type cannot be DECIMAL, or `HLL_UNION(COLUMN)` and the base table is an AGG model.

### Update strategy

In order to ensure the data consistency between the materialized view table and the Base table, Doris will import, delete and other operations on the Base table are synchronized to the materialized view table. And through incremental update to improve update efficiency. To ensure atomicity through transaction.

For example, if the user inserts data into the base table through the INSERT command, this data will be inserted into the materialized view synchronously. When both the base table and the materialized view table are written successfully, the INSERT command will return successfully. To

### Query automatic matching

After the materialized view is successfully created, the user's query does not need to be changed, that is, it is still the base table of the query. Doris will automatically select an optimal materialized view based on the current query statement, read data from the materialized view and calculate it.

Users can use the EXPLAIN command to check whether the current query uses a materialized view.

The matching relationship between the aggregation in the materialized view and the aggregation in the query:

| Materialized View Aggregation | Query Aggregation |
| ---------- | -------- |
| sum | sum |
| min | min |
| max | max |
| count | count |
| bitmap\_union | bitmap\_union, bitmap\_union\_count, count(distinct) |
| hll\_union | hll\_raw\_agg, hll\_union\_agg, ndv, approx\_count\_distinct |

After the aggregation functions of bitmap and hll match the materialized view in the query, the aggregation operator of the query will be rewritten according to the table structure of the materialized view. See example 2 for details.

### Query materialized views

Check what materialized views the current table has, and what their table structure is. Through the following command:

```
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

You can see that the current `mv_test` table has three materialized views: mv\_1, mv\_2 and mv\_3, and their table structure.

### Delete materialized view

If the user no longer needs the materialized view, you can delete the materialized view with the following command:

```
DROP MATERIALIZED VIEW
```

The specific syntax can be viewed through the following command:

```
HELP DROP MATERIALIZED VIEW
```

## Best Practice 1

The use of materialized views is generally divided into the following steps:

1. Create a materialized view
2. Asynchronously check whether the materialized view has been constructed
3. Query and automatically match materialized views

**First is the first step: Create a materialized view**

Assume that the user has a sales record list, which stores the transaction id, salesperson, sales store, sales time, and amount of each transaction. The table building statement is:

```
create table sales_records(record_id int, seller_id int, store_id int, sale_date date, sale_amt bigint) distributed by hash(record_id) properties("replication_num" = "1");
```
The table structure of this `sales_records` is as follows:

```
MySQL [test]> desc sales_records;
+-----------+--------+------+-------+---------+--- ----+
| Field | Type | Null | Key | Default | Extra |
+-----------+--------+------+-------+---------+--- ----+
| record_id | INT | Yes | true | NULL | |
| seller_id | INT | Yes | true | NULL | |
| store_id | INT | Yes | true | NULL | |
| sale_date | DATE | Yes | false | NULL | NONE |
| sale_amt | BIGINT | Yes | false | NULL | NONE |
+-----------+--------+------+-------+---------+--- ----+
```

At this time, if the user often performs an analysis query on the sales volume of different stores, you can create a materialized view for the `sales_records` table to group the sales stores and sum the sales of the same sales stores. The creation statement is as follows:

```
MySQL [test]> create materialized view store_amt as select store_id, sum(sale_amt) from sales_records group by store_id;
```

The backend returns to the following figure, indicating that the task of creating a materialized view is submitted successfully.

```
Query OK, 0 rows affected (0.012 sec)
```

**Step 2: Check whether the materialized view has been built**

Since the creation of a materialized view is an asynchronous operation, after the user submits the task of creating a materialized view, he needs to asynchronously check whether the materialized view has been constructed through a command. The command is as follows:

```
SHOW ALTER TABLE ROLLUP FROM db_name; (Version 0.12)
SHOW ALTER TABLE MATERIALIZED VIEW FROM db_name; (Version 0.13)
```

In this command, `db_name` is a parameter, you need to replace it with your real db name. The result of the command is to display all the tasks of creating a materialized view of this db. The results are as follows:

```
+-------+---------------+---------------------+--- ------------------+---------------+--------------- --+----------+---------------+-----------+-------- -------------------------------------------------- -------------------------------------------------- -------------+----------+---------+
| JobId | TableName | CreateTime | FinishedTime | BaseIndexName | RollupIndexName | RollupId | TransactionId | State | Msg | Progress | Timeout |
+-------+---------------+---------------------+--- ------------------+---------------+--------------- --+----------+---------------+-----------+-------- -------------------------------------------------- -------------------------------------------------- -------------+----------+---------+
| 22036 | sales_records | 2020-07-30 20:04:28 | 2020-07-30 20:04:57 | sales_records | store_amt | 22037 | 5008 | FINISHED | | NULL | 86400 |
+-------+---------------+---------------------+--- ------------------+---------------+--------------- --+----------+---------------+-----------+-------- ----------------------------------------

```

Among them, TableName refers to which table the data of the materialized view comes from, and RollupIndexName refers to the name of the materialized view. One of the more important indicators is State.

When the State of the task of creating a materialized view has become FINISHED, it means that the materialized view has been created successfully. This means that it is possible to automatically match this materialized view when querying.

**Step 3: Query**

After the materialized view is created, when users query the sales volume of different stores, they will directly read the aggregated data from the materialized view `store_amt` just created. To achieve the effect of improving query efficiency.

The user's query still specifies the query `sales_records` table, for example:

```
SELECT store_id, sum(sale_amt) FROM sales_records GROUP BY store_id;
```

The above query will automatically match `store_amt`. The user can use the following command to check whether the current query matches the appropriate materialized view.

```
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
The final thing is the rollup attribute in OlapScanNode. You can see that the rollup of the current query shows `store_amt`. That is to say, the query has been correctly matched to the materialized view `store_amt`, and data is read directly from the materialized view.

## Best Practice 2 PV,UV

Business scenario: Calculate the UV and PV of advertising

Assuming that the user's original ad click data is stored in Doris, then for ad PV and UV queries, the query speed can be improved by creating a materialized view of `bitmap_union`.

Use the following statement to first create a table that stores the details of the advertisement click data, including the click event of each click, what advertisement was clicked, what channel clicked, and who was the user who clicked.

```
MySQL [test]> create table advertiser_view_record(time date, advertiser varchar(10), channel varchar(10), user_id int) distributed by hash(time) properties("replication_num" = "1");
Query OK, 0 rows affected (0.014 sec)
```
The original ad click data table structure is:

```
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

1. Create a materialized view

	Since the user wants to query the UV value of the advertisement, that is, a precise de-duplication of users of the same advertisement is required, the user's query is generally:

	```
	SELECT advertiser, channel, count(distinct user_id) FROM advertiser_view_record GROUP BY advertiser, channel;
	```

	For this kind of UV-seeking scene, we can create a materialized view with `bitmap_union` to achieve a precise deduplication effect in advance.

	In Doris, the result of `count(distinct)` aggregation is exactly the same as the result of `bitmap_union_count` aggregation. And `bitmap_union_count` is equal to the result of `bitmap_union` to calculate count, so if the query ** involves `count(distinct)`, you can speed up the query by creating a materialized view with `bitmap_union` aggregation.**

	For this case, you can create a materialized view that accurately deduplicates `user_id` based on advertising and channel grouping.

	```
	MySQL [test]> create materialized view advertiser_uv as select advertiser, channel, bitmap_union(to_bitmap(user_id)) from advertiser_view_record group by advertiser, channel;
	Query OK, 0 rows affected (0.012 sec)
	```

	*Note: Because the user\_id itself is an INT type, it is called `bitmap_union` directly in Doris. The fields need to be converted to bitmap type through the function `to_bitmap` first, and then `bitmap_union` can be aggregated. *

	After the creation is complete, the table structure of the advertisement click schedule and the materialized view table is as follows:

	```
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

2. Automatic query matching

	When the materialized view table is created, when querying the advertisement UV, Doris will automatically query the data from the materialized view `advertiser_uv` just created. For example, the original query statement is as follows:

	```
	SELECT advertiser, channel, count(distinct user_id) FROM advertiser_view_record GROUP BY advertiser, channel;
	```

	After the materialized view is selected, the actual query will be transformed into:

	```
	SELECT advertiser, channel, bitmap_union_count(to_bitmap(user_id)) FROM advertiser_uv GROUP BY advertiser, channel;
	```

	Through the EXPLAIN command, you can check whether Doris matches the materialized view:

	```
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

	In the result of EXPLAIN, you can first see that the rollup attribute value of OlapScanNode is advertiser_uv. In other words, the query directly scans the data of the materialized view. The match is successful.

	Secondly, the calculation of `count(distinct)` for the `user_id` field is rewritten as `bitmap_union_count`. That is to achieve the effect of precise deduplication through bitmap.


## Best Practice 3

Business scenario: matching a richer prefix index

The user's original table has three columns (k1, k2, k3). Among them, k1, k2 are prefix index columns. At this time, if the user query condition contains `where k1=a and k2=b`, the query can be accelerated through the index.

But in some cases, the user's filter conditions cannot match the prefix index, such as `where k3=c`. Then the query speed cannot be improved through the index.

This problem can be solved by creating a materialized view with k3 as the first column.

1. Create a materialized view

	```
	CREATE MATERIALIZED VIEW mv_1 as SELECT k3, k2, k1 FROM tableA ORDER BY k3;
	```

	After the creation of the above grammar is completed, the complete detail data is retained in the materialized view, and the prefix index of the materialized view is the k3 column. The table structure is as follows:

	```
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

2. Query matching

	At this time, if the user's query has k3 column, the filter condition is, for example:

	```
	select k1, k2, k3 from table A where k3=1;
	```

	At this time, the query will read data directly from the mv_1 materialized view just created. The materialized view has a prefix index on k3, and query efficiency will also be improved.


## Limitations

1. The parameter of the aggregate function of the materialized view does not support the expression only supports a single column, for example: sum(a+b) does not support.
2. If the conditional column of the delete statement does not exist in the materialized view, the delete operation cannot be performed. If you must delete data, you need to delete the materialized view before deleting the data.
3. Too many materialized views on a single table will affect the efficiency of importing: When importing data, the materialized view and base table data are updated synchronously. If a table has more than 10 materialized view tables, it may cause the import speed to be very high. slow. This is the same as a single import needs to import 10 tables at the same time.
4. The same column with different aggregate functions cannot appear in a materialized view at the same time. For example, select sum(a), min(a) from table are not supported.

## Error
1. DATA_QUALITY_ERR: "The data quality does not satisfy, please check your data"
Materialized view creation failed due to data quality issues.
Note: The bitmap type only supports positive integers. If there are negative Numbers in the original data, the materialized view will fail to be created