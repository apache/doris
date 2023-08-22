---
{
    "title": "Rollup and Query",
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

# Rollup and Query

In multidimensional analysis, Rollups refers to data tables that are aggregated at a specified granular level.

### Basic Concepts

In Doris, tables that are created by users via the CREATE TABLE statement are called "Base Tables". Base Tables contains the basic data stored in the way specified by the user in the the CREATE TABLE statement.

On top of Base Tables, you can create any number of Rollups. Data in Rollups are generated based on the Base Tables and are physically **independent in storage**.

Rollups are created to  aggregated versions of Base Tables.

Let's illustrate the ROLLUP tables and their roles in different data models with examples.

#### ROLLUP in Aggregate Model and Uniq Model

Because Uniq is only a special case of the Aggregate model, we do not distinguish it here.

Example 1: Get the total consumption per user

Following [Data Model Aggregate Model](./data-model.md) in the **Aggregate Model** section, the Base table structure is as follows:

| ColumnName        | Type         | AggregationType | Comment                                |
|-------------------| ------------ | --------------- | -------------------------------------- |
| user\_id          | LARGEINT     |                 | user id                                |
| date              | DATE         |                 | date of data filling                   |
| timestamp         | DATETIME     |                 | Data filling time, accurate to seconds |
| city              | VARCHAR (20) |                 | User City                              |
| age               | SMALLINT     |                 | User age                               |
| sex               | TINYINT      |                 | User gender                            |
| last\_visit\_date | DATETIME     | REPLACE         | Last user access time                  |
| cost              | BIGINT       | SUM             | Total User Consumption                 |
| max\_dwell\_time  | INT          | MAX             | Maximum user residence time            |
| min\_dwell\_time  | INT          | MIN             | User minimum residence time            |

The data stored are as follows:

| user\_id | date       | timestamp           | city      | age  | sex  | last\_visit\_date   | cost | max\_dwell\_time | min\_dwell\_time |
|----------| ---------- | ------------------- | --------- | ---- | ---- | ------------------- | ---- | ---------------- | ---------------- |
| 10000    | 2017-10-01 | 2017-10-01 08:00:05 | Beijing   | 20   | 0    | 2017-10-01 06:00    | 20   | 10               | 10               |
| 10000    | 2017-10-01 | 2017-10-01 09:00:05 | Beijing   | 20   | 0    | 2017-10-01 07:00    | 15   | 2                | 2                |
| 10001    | 2017-10-01 | 2017-10-01 18:12:10 | Beijing   | 30   | 1    | 2017-10-01 17:05:45 | 2    | 22               | 22               |
| 10002    | 2017-10-02 | 2017-10-02 13:10:00 | Shanghai  | 20   | 1    | 2017-10-02 12:59:12 | 200  | 5                | 5                |
| 10003    | 2017-10-02 | 2017-10-02 13:15:00 | Guangzhou | 32   | 0    | 2017-10-02 11:20:00 | 30   | 11               | 11               |
| 10004    | 2017-10-01 | 2017-10-01 12:12:48 | Shenzhen  | 35   | 0    | 2017-10-01 10:00:15 | 100  | 3                | 3                |
| 10004    | 2017-10-03 | 2017-10-03 12:38:20 | Shenzhen  | 35   | 0    | 2017-10-03 10:20:22 | 11   | 6                | 6                |

On this basis, we create a ROLLUP:

| ColumnName |
|------------|
| user\_id   |
| cost       |

The ROLLUP contains only two columns: user\_id and cost. After the creation, the data stored in the ROLLUP is as follows:

| user\_id | cost |
| -------- | ---- |
| 10000    | 35   |
| 10001    | 2    |
| 10002    | 200  |
| 10003    | 30   |
| 10004    | 111  |

As you can see, ROLLUP retains only the results of SUM on the cost column for each user_id. So when we do the following query:

`SELECT user_id, sum(cost) FROM table GROUP BY user_id;`

Doris automatically hits the ROLLUP table, thus completing the aggregated query by scanning only a very small amount of data.

2. Example 2: Get the total consumption, the longest and shortest page residence time of users of different ages in different cities

Follow example 1. Based on the Base table, we create a ROLLUP:

| ColumnName       | Type         | AggregationType | Comment                     |
|------------------| ------------ | --------------- | --------------------------- |
| city             | VARCHAR (20) |                 | User City                   |
| age              | SMALLINT     |                 | User age                    |
| cost             | BIGINT       | SUM             | Total User Consumption      |
| max\_dwell\_time | INT          | MAX             | Maximum user residence time |
| min\_dwell\_time | INT          | MIN             | User minimum residence time |

After the creation, the data stored in the ROLLUP is as follows:

| city      | age  | cost | max\_dwell\_time | min\_dwell\_time |
| --------- | ---- | ---- | ---------------- | ---------------- |
| Beijing   | 20   | 35   | 10               | 2                |
| Beijing   | 30   | 2    | 22               | 22               |
| Shanghai  | 20   | 200  | 5                | 5                |
| Guangzhou | 32   | 30   | 11               | 11               |
| Shenzhen  | 35   | 111  | 6                | 3                |

When we do the following queries:

```sql
mysql> SELECT city, age, sum(cost), max(max_dwell_time), min(min_dwell_time) FROM table GROUP BY city, age;
mysql> SELECT city, sum(cost), max(max_dwell_time), min(min_dwell_time) FROM table GROUP BY city;
mysql> SELECT city, age, sum(cost), min(min_dwell_time) FROM table GROUP BY city, age;
```

Doris automatically hits the ROLLUP table.

#### ROLLUP in Duplicate Model

Because the Duplicate model has no aggregate semantics. So the ROLLLUP in this model has lost the meaning of "scroll up". It's just to adjust the column order to hit the prefix index. In the next section, we will introduce prefix index in [data model prefix index](./data-model.md), and how to use ROLLUP to change prefix index in order to achieve better query efficiency.

## ROLLUP adjusts prefix index

Because column order is specified when a table is built, there is only one prefix index for a table. This may be inefficient for queries that use other columns that cannot hit prefix indexes as conditions. Therefore, we can manually adjust the order of columns by creating ROLLUP. Examples are given.

The structure of the Base table is as follows:

| ColumnName       | Type         |
| ---------------- | ------------ |
| user\_id         | BIGINT       |
| age              | INT          |
| message          | VARCHAR(100) |
| max\_dwell\_time | DATETIME     |
| min\_dwell\_time | DATETIME     |

On this basis, we can create a ROLLUP table:

| ColumnName       | Type         |
| ---------------- | ------------ |
| age              | INT          |
| user\_id         | BIGINT       |
| message          | VARCHAR(100) |
| max\_dwell\_time | DATETIME     |
| min\_dwell\_time | DATETIME     |

As you can see, the columns of ROLLUP and Base tables are exactly the same, just changing the order of user_id and age. So when we do the following query:

```sql
mysql> SELECT * FROM table where age=20 and message LIKE "%error%";
```

The ROLLUP table is preferred because the prefix index of ROLLUP matches better.

## Some Explanations of ROLLUP

* The fundamental role of ROLLUP is to improve the query efficiency of some queries (whether by aggregating to reduce the amount of data or by modifying column order to match prefix indexes). Therefore, the meaning of ROLLUP has gone beyond the scope of "roll-up". That's why we named it Materialized Index in the source code.
* ROLLUP is attached to the Base table and can be seen as an auxiliary data structure of the Base table. Users can create or delete ROLLUP based on the Base table, but cannot explicitly specify a query for a ROLLUP in the query. Whether ROLLUP is hit or not is entirely determined by the Doris system.
* ROLLUP data is stored in separate physical storage. Therefore, the more ROLLUP you create, the more disk space you occupy. It also has an impact on the speed of import (the ETL phase of import automatically generates all ROLLUP data), but it does not reduce query efficiency (only better).
* Data updates for ROLLUP are fully synchronized with Base representations. Users need not care about this problem.
* Columns in ROLLUP are aggregated in exactly the same way as Base tables. There is no need to specify or modify ROLLUP when creating it.
* A necessary (inadequate) condition for a query to hit ROLLUP is that all columns ** (including the query condition columns in select list and where) involved in the query exist in the column of the ROLLUP. Otherwise, the query can only hit the Base table.
* Certain types of queries (such as count (*)) cannot hit ROLLUP under any conditions. See the next section **Limitations of the aggregation model**.
* The query execution plan can be obtained by `EXPLAIN your_sql;` command, and in the execution plan, whether ROLLUP has been hit or not can be checked.
* Base tables and all created ROLLUP can be displayed by `DESC tbl_name ALL;` statement.

## Query

As a polymer view in Doris, Rollup can play two roles in queries:

* Index
* Aggregate data (only for aggregate models, aggregate key)

However, in order to hit Rollup, certain conditions need to be met, and the value of PreAggregation of ScanNdo node in the execution plan can be used to determine whether Rollup can be hit or not, and the Rollup field can be used to determine which Rollup table is hit.

### Index

Doris's prefix index has been introduced in the previous query practice, that is, Doris will generate the first 36 bytes in the Base/Rollup table separately in the underlying storage engine (with varchar type, the prefix index may be less than 36 bytes, varchar will truncate the prefix index, and use up to 20 bytes of varchar). A sorted sparse index data (data is also sorted, positioned by index, and then searched by dichotomy in the data), and then matched each Base/Rollup prefix index according to the conditions in the query, and selected a Base/Rollup that matched the longest prefix index.

```
		---> matching from left to right
+----+----+----+----+----+----+
| c1 | c2 | c3 | c4 | c5 |... |
```

As shown in the figure above, the conditions of where and on in the query are pushed up and down to ScanNode and matched from the first column of the prefix index. Check if there are any of these columns in the condition, and then accumulate the matching length until the matching cannot match or the end of 36 bytes (columns of varchar type can only match 20 bytes and match less than 36 words). Section truncates prefix index, and then chooses a Base/Rollup with the longest matching length. The following example shows how to create a Base table and four rollups:

```
+---------------+-------+--------------+------+-------+---------+-------+
| IndexName     | Field | Type         | Null | Key   | Default | Extra |
+---------------+-------+--------------+------+-------+---------+-------+
| test          | k1    | TINYINT      | Yes  | true  | N/A     |       |
|               | k2    | SMALLINT     | Yes  | true  | N/A     |       |
|               | k3    | INT          | Yes  | true  | N/A     |       |
|               | k4    | BIGINT       | Yes  | true  | N/A     |       |
|               | k5    | DECIMAL(9,3) | Yes  | true  | N/A     |       |
|               | k6    | CHAR(5)      | Yes  | true  | N/A     |       |
|               | k7    | DATE         | Yes  | true  | N/A     |       |
|               | k8    | DATETIME     | Yes  | true  | N/A     |       |
|               | k9    | VARCHAR(20)  | Yes  | true  | N/A     |       |
|               | k10   | DOUBLE       | Yes  | false | N/A     | MAX   |
|               | k11   | FLOAT        | Yes  | false | N/A     | SUM   |
|               |       |              |      |       |         |       |
| rollup_index1 | k9    | VARCHAR(20)  | Yes  | true  | N/A     |       |
|               | k1    | TINYINT      | Yes  | true  | N/A     |       |
|               | k2    | SMALLINT     | Yes  | true  | N/A     |       |
|               | k3    | INT          | Yes  | true  | N/A     |       |
|               | k4    | BIGINT       | Yes  | true  | N/A     |       |
|               | k5    | DECIMAL(9,3) | Yes  | true  | N/A     |       |
|               | k6    | CHAR(5)      | Yes  | true  | N/A     |       |
|               | k7    | DATE         | Yes  | true  | N/A     |       |
|               | k8    | DATETIME     | Yes  | true  | N/A     |       |
|               | k10   | DOUBLE       | Yes  | false | N/A     | MAX   |
|               | k11   | FLOAT        | Yes  | false | N/A     | SUM   |
|               |       |              |      |       |         |       |
| rollup_index2 | k9    | VARCHAR(20)  | Yes  | true  | N/A     |       |
|               | k2    | SMALLINT     | Yes  | true  | N/A     |       |
|               | k1    | TINYINT      | Yes  | true  | N/A     |       |
|               | k3    | INT          | Yes  | true  | N/A     |       |
|               | k4    | BIGINT       | Yes  | true  | N/A     |       |
|               | k5    | DECIMAL(9,3) | Yes  | true  | N/A     |       |
|               | k6    | CHAR(5)      | Yes  | true  | N/A     |       |
|               | k7    | DATE         | Yes  | true  | N/A     |       |
|               | k8    | DATETIME     | Yes  | true  | N/A     |       |
|               | k10   | DOUBLE       | Yes  | false | N/A     | MAX   |
|               | k11   | FLOAT        | Yes  | false | N/A     | SUM   |
|               |       |              |      |       |         |       |
| rollup_index3 | k4    | BIGINT       | Yes  | true  | N/A     |       |
|               | k5    | DECIMAL(9,3) | Yes  | true  | N/A     |       |
|               | k6    | CHAR(5)      | Yes  | true  | N/A     |       |
|               | k1    | TINYINT      | Yes  | true  | N/A     |       |
|               | k2    | SMALLINT     | Yes  | true  | N/A     |       |
|               | k3    | INT          | Yes  | true  | N/A     |       |
|               | k7    | DATE         | Yes  | true  | N/A     |       |
|               | k8    | DATETIME     | Yes  | true  | N/A     |       |
|               | k9    | VARCHAR(20)  | Yes  | true  | N/A     |       |
|               | k10   | DOUBLE       | Yes  | false | N/A     | MAX   |
|               | k11   | FLOAT        | Yes  | false | N/A     | SUM   |
|               |       |              |      |       |         |       |
| rollup_index4 | k4    | BIGINT       | Yes  | true  | N/A     |       |
|               | k6    | CHAR(5)      | Yes  | true  | N/A     |       |
|               | k5    | DECIMAL(9,3) | Yes  | true  | N/A     |       |
|               | k1    | TINYINT      | Yes  | true  | N/A     |       |
|               | k2    | SMALLINT     | Yes  | true  | N/A     |       |
|               | k3    | INT          | Yes  | true  | N/A     |       |
|               | k7    | DATE         | Yes  | true  | N/A     |       |
|               | k8    | DATETIME     | Yes  | true  | N/A     |       |
|               | k9    | VARCHAR(20)  | Yes  | true  | N/A     |       |
|               | k10   | DOUBLE       | Yes  | false | N/A     | MAX   |
|               | k11   | FLOAT        | Yes  | false | N/A     | SUM   |
+---------------+-------+--------------+------+-------+---------+-------+
```

The prefix indexes of the five tables are

```
Base(k1 ,k2, k3, k4, k5, k6, k7)

rollup_index1(k9)

rollup_index2(k9)

rollup_index3(k4, k5, k6, k1, k2, k3, k7)

rollup_index4(k4, k6, k5, k1, k2, k3, k7)
```

Conditions on columns that can be indexed with the prefix need to be `=` `<` `>` `<=` `>=` `in` `between`, and these conditions are side-by-side and the relationship uses `and` connections', which cannot be hit for `or`、`!=` and so on. Then look at the following query:

```sql
SELECT * FROM test WHERE k1 = 1 AND k2 > 3;
```

With the conditions on K1 and k2, check that only the first column of Base contains K1 in the condition, so match the longest prefix index, test, explain:

```text
|   0:OlapScanNode                                                                                                                                                                                                                                                                                                                                                                                                 
|      TABLE: test                                                                                                                                                                                                                                                                                                                                                                                                  
|      PREAGGREGATION: OFF. Reason: No AggregateInfo                                                                                                                                                                                                                                                                                                                                                                
|      PREDICATES: `k1` = 1, `k2` > 3                                                                                                                                                                                                                                                                                                                                                                               
|      partitions=1/1                                                                                                                                                                                                                                                                                                                                                                                               
|      rollup: test                                                                                                                                                                                                                                                                                                                                                                                                 
|      buckets=1/10                                                                                                                                                                                                                                                                                                                                                                                                 
|      cardinality=-1                                                                                                                                                                                                                                                                                                                                                                                               
|      avgRowSize=0.0                                                                                                                                                                                                                                                                                                                                                                                               
|      numNodes=0                                                                                                                                                                                                                                                                                                                                                                                                   
|      tuple ids: 0
```

Look again at the following queries:

```sql
SELECT * FROM test WHERE k4 = 1 AND k5 > 3;
```

With K4 and K5 conditions, check that the first column of rollup_index3 and rollup_index4 contains k4, but the second column of rollup_index3 contains k5, so the matching prefix index is the longest.

```
|   0:OlapScanNode                                                                                                                                                                                                                                                                                                                                                                                                
|      TABLE: test                                                                                                                                                                                                                                                                                                                                                                                                  
|      PREAGGREGATION: OFF. Reason: No AggregateInfo                                                                                                                                                                                                                                                                                                                                                                
|      PREDICATES: `k4` = 1, `k5` > 3                                                                                                                                                                                                                                                                                                                                                                              
|      partitions=1/1                                                                                                                                                                                                                                                                                                                                                                                               
|      rollup: rollup_index3                                                                                                                                                                                                                                                                                                                                                                                        
|      buckets=10/10                                                                                                                                                                                                                                                                                                                                                                                                
|      cardinality=-1                                                                                                                                                                                                                                                                                                                                                                                               
|      avgRowSize=0.0                                                                                                                                                                                                                                                                                                                                                                                               
|      numNodes=0                                                                                                                                                                                                                                                                                                                                                                                                   
|      tuple ids: 0
```

Now we try to match the conditions on the column containing varchar, as follows:

```sql
SELECT * FROM test WHERE k9 IN ("xxx", "yyyy") AND k1 = 10;
```

There are K9 and K1 conditions. The first column of rollup_index1 and rollup_index2 contains k9. It is reasonable to choose either rollup here to hit the prefix index and randomly select the same one (because there are just 20 bytes in varchar, and the prefix index is truncated in less than 36 bytes). The current strategy here will continue to match k1, because the second rollup_index1 is listed as k1, so rollup_index1 is chosen, in fact, the latter K1 condition will not play an accelerating role. (If the condition outside the prefix index needs to accelerate the query, it can be accelerated by establishing a Bloom Filter filter. Typically for string types, because Doris has a Block level for columns, a Min/Max index for shaping and dates.) The following is the result of explain.

```
|   0:OlapScanNode                                                                                                                                                                                                                                                                                                                                                                                                  
|      TABLE: test                                                                                                                                                                                                                                                                                                                                                                                                  
|      PREAGGREGATION: OFF. Reason: No AggregateInfo                                                                                                                                                                                                                                                                                                                                                                
|      PREDICATES: `k9` IN ('xxx', 'yyyy'), `k1` = 10                                                                                                                                                                                                                                                                                                                                                               
|      partitions=1/1                                                                                                                                                                                                                                                                                                                                                                                               
|      rollup: rollup_index1                                                                                                                                                                                                                                                                                                                                                                                        
|      buckets=1/10                                                                                                                                                                                                                                                                                                                                                                                                 
|      cardinality=-1                                                                                                                                                                                                                                                                                                                                                                                               
|      avgRowSize=0.0                                                                                                                                                                                                                                                                                                                                                                                               
|      numNodes=0                                                                                                                                                                                                                                                                                                                                                                                                   
|      tuple ids: 0
```

Finally, look at a query that can be hit by more than one Rollup:

```sql
SELECT * FROM test WHERE K4 < 1000 AND K5 = 80 AND K6 = 10000;
```

There are three conditions: k4, K5 and k6. The first three columns of rollup_index3 and rollup_index4 contain these three columns respectively. So the prefix index length matched by them is the same. Both can be selected. The current default strategy is to select a rollup created earlier. Here is rollup_index3.

```
|   0:OlapScanNode                                                                                                                                                                                                                                                                                                                                                                                                  
|      TABLE: test                                                                                                                                                                                                                                                                                                                                                                                                  
|      PREAGGREGATION: OFF. Reason: No AggregateInfo                                                                                                                                                                                                                                                                                                                                                                
|      PREDICATES: `k4` < 1000, `k5` = 80, `k6` >= 10000.0                                                                                                                                                                                                                                                                                                                                                          
|      partitions=1/1                                                                                                                                                                                                                                                                                                                                                                                               
|      rollup: rollup_index3                                                                                                                                                                                                                                                                                                                                                                                        
|      buckets=10/10                                                                                                                                                                                                                                                                                                                                                                                                
|      cardinality=-1                                                                                                                                                                                                                                                                                                                                                                                               
|      avgRowSize=0.0                                                                                                                                                                                                                                                                                                                                                                                               
|      numNodes=0                                                                                                                                                                                                                                                                                                                                                                                                   
|      tuple ids: 0
```

If you modify the above query slightly as follows:

```sql
SELECT * FROM test WHERE k4 < 1000 AND k5 = 80 OR k6 >= 10000;
```

The query here cannot hit the prefix index. (Even any Min/Max in the Doris storage engine, the BloomFilter index doesn't work.)

### Aggregate data

Of course, the function of aggregated data is indispensable for general polymer views. Such materialized views are very helpful for aggregated queries or report queries. To hit the polymer views, the following prerequisites are needed:

1. There is a separate Rollup for all columns involved in a query or subquery.
2. If there is Join in a query or sub-query, the type of Join needs to be Inner join.

The following are some types of aggregated queries that can hit Rollup.

| Column type Query type |  Sum  | Distinct/Count Distinct |   Min |  Max  |  APPROX_COUNT_DISTINCT  |
|--------------|-------|-------------------------|-------|-------|-------|
|     Key      | false |           true          |  true |  true | true  |
|   Value(Sum) |  true |          false          | false | false | false |
|Value(Replace)| false |          false          | false | false | false |
|   Value(Min) | false |          false          |  true | false | false |
|   Value(Max) | false |          false          | false | true  | false |


If the above conditions are met, there will be two stages in judging the hit of Rollup for the aggregation model:

1. Firstly, the Rollup table with the longest index hit by prefix index is matched by conditions. See the index strategy above.
2. Then compare the rows of Rollup and select the smallest Rollup.

The following Base table and Rollup:

```
+-------------+-------+--------------+------+-------+---------+-------+
| IndexName   | Field | Type         | Null | Key   | Default | Extra |
+-------------+-------+--------------+------+-------+---------+-------+
| test_rollup | k1    | TINYINT      | Yes  | true  | N/A     |       |
|             | k2    | SMALLINT     | Yes  | true  | N/A     |       |
|             | k3    | INT          | Yes  | true  | N/A     |       |
|             | k4    | BIGINT       | Yes  | true  | N/A     |       |
|             | k5    | DECIMAL(9,3) | Yes  | true  | N/A     |       |
|             | k6    | CHAR(5)      | Yes  | true  | N/A     |       |
|             | k7    | DATE         | Yes  | true  | N/A     |       |
|             | k8    | DATETIME     | Yes  | true  | N/A     |       |
|             | k9    | VARCHAR(20)  | Yes  | true  | N/A     |       |
|             | k10   | DOUBLE       | Yes  | false | N/A     | MAX   |
|             | k11   | FLOAT        | Yes  | false | N/A     | SUM   |
|             |       |              |      |       |         |       |
| rollup2     | k1    | TINYINT      | Yes  | true  | N/A     |       |
|             | k2    | SMALLINT     | Yes  | true  | N/A     |       |
|             | k3    | INT          | Yes  | true  | N/A     |       |
|             | k10   | DOUBLE       | Yes  | false | N/A     | MAX   |
|             | k11   | FLOAT        | Yes  | false | N/A     | SUM   |
|             |       |              |      |       |         |       |
| rollup1     | k1    | TINYINT      | Yes  | true  | N/A     |       |
|             | k2    | SMALLINT     | Yes  | true  | N/A     |       |
|             | k3    | INT          | Yes  | true  | N/A     |       |
|             | k4    | BIGINT       | Yes  | true  | N/A     |       |
|             | k5    | DECIMAL(9,3) | Yes  | true  | N/A     |       |
|             | k10   | DOUBLE       | Yes  | false | N/A     | MAX   |
|             | k11   | FLOAT        | Yes  | false | N/A     | SUM   |
+-------------+-------+--------------+------+-------+---------+-------+
```


See the following queries:

```sql
SELECT SUM(k11) FROM test_rollup WHERE k1 = 10 AND k2 > 200 AND k3 in (1,2,3);
```

Firstly, it judges whether the query can hit the aggregated Rollup table. After checking the graph above, it is possible. Then the condition contains three conditions: k1, K2 and k3. The first three columns of test_rollup, rollup1 and rollup2 contain all the three conditions. So the prefix index length is the same. Then, it is obvious that the aggregation degree of rollup2 is the highest when comparing the number of rows. Row 2 is selected because of the minimum number of rows.

```
|   0:OlapScanNode                                          |
|      TABLE: test_rollup                                   |
|      PREAGGREGATION: ON                                   |
|      PREDICATES: `k1` = 10, `k2` > 200, `k3` IN (1, 2, 3) |
|      partitions=1/1                                       |
|      rollup: rollup2                                      |
|      buckets=1/10                                         |
|      cardinality=-1                                       |
|      avgRowSize=0.0                                       |
|      numNodes=0                                           |
|      tuple ids: 0                                         |
```
