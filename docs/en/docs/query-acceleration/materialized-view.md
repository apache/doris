---
{
    "title": "Materialized View",
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

# Materialized View
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

Creating a materialized view is an asynchronous operation, which means that after the user successfully submits the creation task, Doris will calculate the existing data in the background until the creation is successful.

The specific syntax can be viewed through the following command:

```
HELP CREATE MATERIALIZED VIEW
```

<version since="2.0.0"></version>

In `Doris 2.0` we made some enhancements to materialized views (described in `Best Practice 4` of this article). We recommend that users check whether the expected query can hit the desired materialized view in the test environment before using the materialized view in the official production environment.

If you don't know how to verify that a query hits a materialized view, you can read `Best Practice 1` of this article.

At the same time, we do not recommend that users create multiple materialized views with similar shapes on the same table, which may cause conflicts between multiple materialized views and cause query hit failures (this problem will be improved in the new optimizer ). It is recommended that users first verify whether materialized views and queries meet the requirements and can be used normally in the test environment.
### Support aggregate functions

The aggregate functions currently supported by the materialized view function are:

+ SUM, MIN, MAX (Version 0.12)
+ COUNT, BITMAP\_UNION, HLL\_UNION (Version 0.13)

### Update strategy

In order to ensure the data consistency between the materialized view table and the Base table, Doris will import, delete and other operations on the Base table are synchronized to the materialized view table. And through incremental update to improve update efficiency. To ensure atomicity through transaction.

For example, if the user inserts data into the base table through the INSERT command, this data will be inserted into the materialized view synchronously. When both the base table and the materialized view table are written successfully, the INSERT command will return successfully.

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

If the user no longer needs the materialized view, you can delete the materialized view by 'DROP' commen.

You can view the specific syntax[SHOW CREATE MATERIALIZED VIEW](../sql-manual/sql-reference/Data-Definition-Statements/Drop/DROP-MATERIALIZED-VIEW.md)

### View the materialized view that has been created

Users can view the created materialized views by using commands

You can view the specific syntax[SHOW CREATE MATERIALIZED VIEW](../sql-manual/sql-reference/Show-Statements/SHOW-CREATE-MATERIALIZED-VIEW.md)

### Cancel Create materialized view

```text
CANCEL ALTER TABLE MATERIALIZED VIEW FROM db_name.table_name
```


## Best Practice 1

The use of materialized views is generally divided into the following steps:

1. Create a materialized view
2. Asynchronously check whether the materialized view has been constructed
3. Query and automatically match materialized views

**First is the first step: Create a materialized view**

Assume that the user has a sales record list, which stores the transaction id, salesperson, sales store, sales time, and amount of each transaction. The table building statement and insert data statement is:

```
create table sales_records(record_id int, seller_id int, store_id int, sale_date date, sale_amt bigint) distributed by hash(record_id) properties("replication_num" = "1");
insert into sales_records values(1,1,1,"2020-02-02",1);
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

```sql
EXPLAIN SELECT store_id, sum(sale_amt) FROM sales_records GROUP BY store_id;
+----------------------------------------------------------------------------------------------+
| Explain String                                                                               |
+----------------------------------------------------------------------------------------------+
| PLAN FRAGMENT 0                                                                              |
|   OUTPUT EXPRS:                                                                              |
|     <slot 4> `default_cluster:test`.`sales_records`.`mv_store_id`                            |
|     <slot 5> sum(`default_cluster:test`.`sales_records`.`mva_SUM__`sale_amt``)               |
|   PARTITION: UNPARTITIONED                                                                   |
|                                                                                              |
|   VRESULT SINK                                                                               |
|                                                                                              |
|   4:VEXCHANGE                                                                                |
|      offset: 0                                                                               |
|                                                                                              |
| PLAN FRAGMENT 1                                                                              |
|                                                                                              |
|   PARTITION: HASH_PARTITIONED: <slot 4> `default_cluster:test`.`sales_records`.`mv_store_id` |
|                                                                                              |
|   STREAM DATA SINK                                                                           |
|     EXCHANGE ID: 04                                                                          |
|     UNPARTITIONED                                                                            |
|                                                                                              |
|   3:VAGGREGATE (merge finalize)                                                              |
|   |  output: sum(<slot 5> sum(`default_cluster:test`.`sales_records`.`mva_SUM__`sale_amt``)) |
|   |  group by: <slot 4> `default_cluster:test`.`sales_records`.`mv_store_id`                 |
|   |  cardinality=-1                                                                          |
|   |                                                                                          |
|   2:VEXCHANGE                                                                                |
|      offset: 0                                                                               |
|                                                                                              |
| PLAN FRAGMENT 2                                                                              |
|                                                                                              |
|   PARTITION: HASH_PARTITIONED: `default_cluster:test`.`sales_records`.`record_id`            |
|                                                                                              |
|   STREAM DATA SINK                                                                           |
|     EXCHANGE ID: 02                                                                          |
|     HASH_PARTITIONED: <slot 4> `default_cluster:test`.`sales_records`.`mv_store_id`          |
|                                                                                              |
|   1:VAGGREGATE (update serialize)                                                            |
|   |  STREAMING                                                                               |
|   |  output: sum(`default_cluster:test`.`sales_records`.`mva_SUM__`sale_amt``)               |
|   |  group by: `default_cluster:test`.`sales_records`.`mv_store_id`                          |
|   |  cardinality=-1                                                                          |
|   |                                                                                          |
|   0:VOlapScanNode                                                                            |
|      TABLE: default_cluster:test.sales_records(store_amt), PREAGGREGATION: ON                |
|      partitions=1/1, tablets=10/10, tabletList=50028,50030,50032 ...                         |
|      cardinality=1, avgRowSize=1520.0, numNodes=1                                            |
+----------------------------------------------------------------------------------------------+
```
From the bottom `test.sales_records(store_amt)`, it can be shown that this query hits the `store_amt` materialized view. It is worth noting that if there is no data in the table, then the materialized view may not be hit.

## Best Practice 2 PV,UV

Business scenario: Calculate the UV and PV of advertising

Assuming that the user's original ad click data is stored in Doris, then for ad PV and UV queries, the query speed can be improved by creating a materialized view of `bitmap_union`.

Use the following statement to first create a table that stores the details of the advertisement click data, including the click event of each click, what advertisement was clicked, what channel clicked, and who was the user who clicked.

```
MySQL [test]> create table advertiser_view_record(time date, advertiser varchar(10), channel varchar(10), user_id int) distributed by hash(time) properties("replication_num" = "1");
insert into advertiser_view_record values("2020-02-02",'a','a',1);
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

	For this case, you can create a materialized view that accurately deduplicate `user_id` based on advertising and channel grouping.

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

   ```sql
   mysql [test]>explain SELECT advertiser, channel, count(distinct user_id) FROM  advertiser_view_record GROUP BY advertiser, channel;
   +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
   | Explain String                                                                                                                                                                 |
   +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
   | PLAN FRAGMENT 0                                                                                                                                                                |
   |   OUTPUT EXPRS:                                                                                                                                                                |
   |     <slot 9> `default_cluster:test`.`advertiser_view_record`.`mv_advertiser`                                                                                                   |
   |     <slot 10> `default_cluster:test`.`advertiser_view_record`.`mv_channel`                                                                                                     |
   |     <slot 11> bitmap_union_count(`default_cluster:test`.`advertiser_view_record`.`mva_BITMAP_UNION__to_bitmap_with_check(`user_id`)`)                                          |
   |   PARTITION: UNPARTITIONED                                                                                                                                                     |
   |                                                                                                                                                                                |
   |   VRESULT SINK                                                                                                                                                                 |
   |                                                                                                                                                                                |
   |   4:VEXCHANGE                                                                                                                                                                  |
   |      offset: 0                                                                                                                                                                 |
   |                                                                                                                                                                                |
   | PLAN FRAGMENT 1                                                                                                                                                                |
   |                                                                                                                                                                                |
   |   PARTITION: HASH_PARTITIONED: <slot 6> `default_cluster:test`.`advertiser_view_record`.`mv_advertiser`, <slot 7> `default_cluster:test`.`advertiser_view_record`.`mv_channel` |
   |                                                                                                                                                                                |
   |   STREAM DATA SINK                                                                                                                                                             |
   |     EXCHANGE ID: 04                                                                                                                                                            |
   |     UNPARTITIONED                                                                                                                                                              |
   |                                                                                                                                                                                |
   |   3:VAGGREGATE (merge finalize)                                                                                                                                                |
   |   |  output: bitmap_union_count(<slot 8> bitmap_union_count(`default_cluster:test`.`advertiser_view_record`.`mva_BITMAP_UNION__to_bitmap_with_check(`user_id`)`))              |
   |   |  group by: <slot 6> `default_cluster:test`.`advertiser_view_record`.`mv_advertiser`, <slot 7> `default_cluster:test`.`advertiser_view_record`.`mv_channel`                 |
   |   |  cardinality=-1                                                                                                                                                            |
   |   |                                                                                                                                                                            |
   |   2:VEXCHANGE                                                                                                                                                                  |
   |      offset: 0                                                                                                                                                                 |
   |                                                                                                                                                                                |
   | PLAN FRAGMENT 2                                                                                                                                                                |
   |                                                                                                                                                                                |
   |   PARTITION: HASH_PARTITIONED: `default_cluster:test`.`advertiser_view_record`.`time`                                                                                          |
   |                                                                                                                                                                                |
   |   STREAM DATA SINK                                                                                                                                                             |
   |     EXCHANGE ID: 02                                                                                                                                                            |
   |     HASH_PARTITIONED: <slot 6> `default_cluster:test`.`advertiser_view_record`.`mv_advertiser`, <slot 7> `default_cluster:test`.`advertiser_view_record`.`mv_channel`          |
   |                                                                                                                                                                                |
   |   1:VAGGREGATE (update serialize)                                                                                                                                              |
   |   |  STREAMING                                                                                                                                                                 |
   |   |  output: bitmap_union_count(`default_cluster:test`.`advertiser_view_record`.`mva_BITMAP_UNION__to_bitmap_with_check(`user_id`)`)                                           |
   |   |  group by: `default_cluster:test`.`advertiser_view_record`.`mv_advertiser`, `default_cluster:test`.`advertiser_view_record`.`mv_channel`                                   |
   |   |  cardinality=-1                                                                                                                                                            |
   |   |                                                                                                                                                                            |
   |   0:VOlapScanNode                                                                                                                                                              |
   |      TABLE: default_cluster:test.advertiser_view_record(advertiser_uv), PREAGGREGATION: ON                                                                                     |
   |      partitions=1/1, tablets=10/10, tabletList=50075,50077,50079 ...                                                                                                           |
   |      cardinality=0, avgRowSize=48.0, numNodes=1                                                                                                                                |
   +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
   ```

	In the result of EXPLAIN, you can first see that `VOlapScanNode` hits `advertiser_uv`. That is, the query scans the materialized view's data directly. Indicates that the match is successful.

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

## Best Practice 4

<version since="2.0.0"></version>

In `Doris 2.0`, we have made some enhancements to the expressions supported by the materialized view. This example will mainly reflect the support and early filtering of the new version of the materialized view for various expressions.

1. Create a base table and insert some data.
    ```sql
    create table d_table (
       k1 int null,
       k2 int not null,
       k3 bigint null,
       k4 date null
    )
    duplicate key (k1,k2,k3)
    distributed BY hash(k1) buckets 3
    properties("replication_num" = "1");

    insert into d_table select 1,1,1,'2020-02-20';
    insert into d_table select 2,2,2,'2021-02-20';
    insert into d_table select 3,-3,null,'2022-02-20';
    ```
   
2. Create some materialized views.
    ```sql
    create materialized view k1a2p2ap3ps as select abs(k1)+k2+1,sum(abs(k2+2)+k3+3) from d_table group by abs(k1)+k2+1;
    create materialized view kymd as select year(k4),month(k4) from d_table where year(k4) = 2020; // Filter with where expression in advance to reduce the amount of data in the materialized view.
    ```

3. Use some queries to test if the materialized view was successfully hit.
    ```sql
    select abs(k1)+k2+1, sum(abs(k2+2)+k3+3) from d_table group by abs(k1)+k2+1; // hit k1a2p2ap3ps
    select bin(abs(k1)+k2+1), sum(abs(k2+2)+k3+3) from d_table group by bin(abs(k1)+k2+1); // hit k1a2p2ap3ps
    select year(k4),month(k4),day(k4) from d_table; // cannot hit the materialized view because the where condition does not match
    select year(k4)+month(k4) from d_table where year(k4) = 2020; // hit kymd
    ```

## Limitations

1. If the condition column of the delete statement does not exist in the materialized view, the delete operation cannot be performed. If you must delete the data, you need to delete the materialized view before deleting the data.
2. Too many materialized views on a single table will affect the efficiency of import: when importing data, the materialized view and Base table data are updated synchronously. If a table has more than 10 materialized views, the import speed may be slow. slow. This is the same as if a single import needs to import 10 table data at the same time.
3. For the Unique Key data model, the materialized view can only change the order of the columns and cannot perform aggregation. Therefore, it is not possible to perform coarse-grained aggregation operations on the data by creating a materialized view on the Unique Key model.
4. At present, the rewriting behavior of some optimizers to SQL may cause the materialized view to fail to be hit. For example, k1+1-1 is rewritten as k1, between is rewritten as <= and >=, and day is rewritten as dayofmonth. In this case, you need to manually adjust the statements of the query and materialized view.

## Error
1. DATA_QUALITY_ERROR: "The data quality does not satisfy, please check your data"
Materialized view creation failed due to data quality issues or Schema Change memory usage exceeding the limit. If it is a memory problem, increase the `memory_limitation_per_thread_for_schema_change_bytes` parameter.
Note: The bitmap type only supports positive integers. If there are negative Numbers in the original data, the materialized view will fail to be created

## More Help

For more detailed syntax and best practices for using materialized views, see [CREATE MATERIALIZED VIEW](../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-MATERIALIZED-VIEW.md) and [DROP MATERIALIZED VIEW](../sql-manual/sql-reference/Data-Definition-Statements/Drop/DROP-MATERIALIZED-VIEW.md) command manual, you can also Enter `HELP CREATE MATERIALIZED VIEW` and `HELP DROP MATERIALIZED VIEW` at the command line of the MySql client for more help information.
