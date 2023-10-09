---
{
    "title": "Data Model",
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

# Data Model

This topic introduces the data models in Doris from a logical perspective so you can make better use of Doris in different business scenarios.

## Basic Concepts

In Doris, data is logically described in the form of tables. A table consists of rows and columns. Row is a row of user data. Column is used to describe different fields in a row of data.

Columns can be divided into two categories: Key and Value. From a business perspective, Key and Value correspond to dimension columns and indicator columns, respectively. The key column of Doris is the column specified in the table creation statement. The column after the keyword 'unique key' or 'aggregate key' or 'duplicate key' in the table creation statement is the key column, and the rest except the key column is the value column .

Data models in Doris fall into three types:

* Aggregate
* Unique
* Duplicate

The following is the detailed introduction to each of them.

## Aggregate Model

We illustrate what aggregation model is and how to use it correctly with practical examples.

### Example 1: Importing Data Aggregation

Assume that the business has the following data table schema:

| ColumnName      | Type         | AggregationType | Comment                     |
|-----------------|--------------|-----------------|-----------------------------|
| userid          | LARGEINT     |                 | user id                     |
| date            | DATE         |                 | date of data filling        |
| City            | VARCHAR (20) |                 | User City                   |
| age             | SMALLINT     |                 | User age                    |
| sex             | TINYINT      |                 | User gender                 |
| Last_visit_date | DATETIME     | REPLACE         | Last user access time       |
| Cost            | BIGINT       | SUM             | Total User Consumption      |
| max dwell time  | INT          | MAX             | Maximum user residence time |
| min dwell time  | INT          | MIN             | User minimum residence time |

The corresponding to CREATE TABLE statement would be as follows (omitting the Partition and Distribution information):

```
CREATE DATABASE IF NOT EXISTS example_db;

CREATE TABLE IF NOT EXISTS example_db.example_tbl_agg1
(
    `user_id` LARGEINT NOT NULL COMMENT "user id",
    `date` DATE NOT NULL COMMENT "data import time",
    `city` VARCHAR(20) COMMENT "city",
    `age` SMALLINT COMMENT "age",
    `sex` TINYINT COMMENT "gender",
    `last_visit_date` DATETIME REPLACE DEFAULT "1970-01-01 00:00:00" COMMENT "last visit date time",
    `cost` BIGINT SUM DEFAULT "0" COMMENT "user total cost",
    `max_dwell_time` INT MAX DEFAULT "0" COMMENT "user max dwell time",
    `min_dwell_time` INT MIN DEFAULT "99999" COMMENT "user min dwell time"
)
AGGREGATE KEY(`user_id`, `date`, `city`, `age`, `sex`)
DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1"
);
```

As you can see, this is a typical fact table of user information and visit behaviors. In star models, user information and visit behaviors are usually stored in dimension tables and fact tables, respectively. Here, for the convenience of explanation, we store the two types of information in one single table.

The columns in the table are divided into Key (dimension) columns and Value (indicator columns) based on whether they are set with an `AggregationType`. **Key** columns are not set with an  `AggregationType`, such as `user_id`, `date`, and  `age`, while **Value** columns are.

When data are imported, rows with the same contents in the Key columns will be aggregated into one row, and their values in the Value columns will be aggregated as their `AggregationType` specify. Currently, there are several aggregation methods and "agg_state" options available:

1. SUM: Accumulate the values in multiple rows.
2. REPLACE: The newly imported value will replace the previous value.
3. MAX: Keep the maximum value.
4. MIN: Keep the minimum value.
5. REPLACE_IF_NOT_NULL: Non-null value replacement. Unlike REPLACE, it does not replace null values.
6. HLL_UNION: Aggregation method for columns of HLL type, using the HyperLogLog algorithm for aggregation.
7. BITMAP_UNION: Aggregation method for columns of BITMAP type, performing a union aggregation of bitmaps.

If these aggregation methods cannot meet the requirements, you can choose to use the "agg_state" type.

Suppose that you have the following import data (raw data):

| user\_id | date       | city      | age | sex | last\_visit\_date   | cost | max\_dwell\_time | min\_dwell\_time |
|----------|------------|-----------|-----|-----|---------------------|------|------------------|------------------|
| 10000    | 2017-10-01 | Beijing   | 20  | 0   | 2017-10-01 06:00    | 20   | 10               | 10               |
| 10000    | 2017-10-01 | Beijing   | 20  | 0   | 2017-10-01 07:00    | 15   | 2                | 2                |
| 10001    | 2017-10-01 | Beijing   | 30  | 1   | 2017-10-01 17:05:45 | 2    | 22               | 22               |
| 10002    | 2017-10-02 | Shanghai  | 20  | 1   | 2017-10-02 12:59:12 | 200  | 5                | 5                |
| 10003    | 2017-10-02 | Guangzhou | 32  | 0   | 2017-10-02 11:20:00 | 30   | 11               | 11               |
| 10004    | 2017-10-01 | Shenzhen  | 35  | 0   | 2017-10-01 10:00:15 | 100  | 3                | 3                |
| 10004    | 2017-10-03 | Shenzhen  | 35  | 0   | 2017-10-03 10:20:22 | 11   | 6                | 6                |


And you can import data with the following sql:

```sql
insert into example_db.example_tbl_agg1 values
(10000,"2017-10-01","Beijing",20,0,"2017-10-01 06:00:00",20,10,10),
(10000,"2017-10-01","Beijing",20,0,"2017-10-01 07:00:00",15,2,2),
(10001,"2017-10-01","Beijing",30,1,"2017-10-01 17:05:45",2,22,22),
(10002,"2017-10-02","Shanghai",20,1,"2017-10-02 12:59:12",200,5,5),
(10003,"2017-10-02","Guangzhou",32,0,"2017-10-02 11:20:00",30,11,11),
(10004,"2017-10-01","Shenzhen",35,0,"2017-10-01 10:00:15",100,3,3),
(10004,"2017-10-03","Shenzhen",35,0,"2017-10-03 10:20:22",11,6,6);
```

Assume that this is a table recording the user behaviors when visiting a certain commodity page. The first row of data, for example, is explained as follows:

| Data             | Description                                               |
|------------------|-----------------------------------------------------------|
| 10000            | User id, each user uniquely identifies id                 |
| 2017-10-01       | Data storage time, accurate to date                       |
| Beijing          | User City                                                 |
| 20               | User Age                                                  |
| 0                | Gender male (1 for female)                                |
| 2017-10-01 06:00 | User's time to visit this page, accurate to seconds       |
| 20               | Consumption generated by the user's current visit         |
| 10               | User's visit, time to stay on the page                    |
| 10               | User's current visit, time spent on the page (redundancy) |

After this batch of data is imported into Doris correctly, it will be stored in Doris as follows:

| user\_id | date       | city      | age | sex | last\_visit\_date   | cost | max\_dwell\_time | min\_dwell\_time |
|----------|------------|-----------|-----|-----|---------------------|------|------------------|------------------|
| 10000    | 2017-10-01 | Beijing   | 20  | 0   | 2017-10-01 07:00    | 35   | 10               | 2                |
| 10001    | 2017-10-01 | Beijing   | 30  | 1   | 2017-10-01 17:05:45 | 2    | 22               | 22               |
| 10002    | 2017-10-02 | Shanghai  | 20  | 1   | 2017-10-02 12:59:12 | 200  | 5                | 5                |
| 10003    | 2017-10-02 | Guangzhou | 32  | 0   | 2017-10-02 11:20:00 | 30   | 11               | 11               |
| 10004    | 2017-10-01 | Shenzhen  | 35  | 0   | 2017-10-01 10:00:15 | 100  | 3                | 3                |
| 10004    | 2017-10-03 | Shenzhen  | 35  | 0   | 2017-10-03 10:20:22 | 11   | 6                | 6                |

As you can see, the data of User 10000 have been aggregated to one row, while those of other users remain the same. The explanation for the aggregated data of User 10000 is as follows (the first 5 columns remain unchanged, so it starts with Column 6 `last_visit_date`):

*`2017-10-01 07:00`: The `last_visit_date` column is aggregated by REPLACE, so `2017-10-01 07:00` has replaced  `2017-10-01 06:00`.

> Note: When using REPLACE to aggregate data from the same import batch, the order of replacement is uncertain. That means, in this case, the data eventually saved in Doris could be `2017-10-01 06:00`. However, for different import batches, it is certain that data from the new batch will replace those from the old batch.

*`35`: The `cost`column is aggregated by SUM, so the update value `35` is the result of `20` + `15`.

*`10`: The `max_dwell_time` column is aggregated by MAX, so `10` is saved as it is the maximum between `10` and `2`.

*`2`: The  `min_dwell_time` column is aggregated by MIN, so `2` is saved as it is the minimum between `10` and `2`.

After aggregation, Doris only stores the aggregated data. In other words, the detailed raw data will no longer be available.

### Example 2: Keep Detailed Data

Here is a modified version of the table schema in Example 1:

| ColumnName      | Type         | AggregationType | Comment                                                               |
|-----------------|--------------|-----------------|-----------------------------------------------------------------------|
| user_id         | LARGEINT     |                 | User ID                                                               |
| date            | DATE         |                 | Date when the data are imported                                       |
| timestamp       | DATETIME     |                 | Date and time when the data are imported (with second-level accuracy) |
| city            | VARCHAR (20) |                 | User location city                                                    |
| age             | SMALLINT     |                 | User age                                                              |
| sex             | TINYINT      |                 | User gender                                                           |
| last visit date | DATETIME     | REPLACE         | Last visit time of the user                                           |
| cost            | BIGINT       | SUM             | Total consumption of the user                                         |
| max_dwell_time  | INT          | MAX             | Maximum user dwell time                                               |
| min_dwell_time  | INT          | MIN             | Minimum user dwell time                                               |

A new column  `timestamp` has been added to record the date and time when the data are imported (with second-level accuracy).

```sql
CREATE TABLE IF NOT EXISTS example_db.example_tbl_agg2
(
    `user_id` LARGEINT NOT NULL COMMENT "用户id",
    `date` DATE NOT NULL COMMENT "数据灌入日期时间",
	`timestamp` DATETIME NOT NULL COMMENT "数据灌入日期时间戳",
    `city` VARCHAR(20) COMMENT "用户所在城市",
    `age` SMALLINT COMMENT "用户年龄",
    `sex` TINYINT COMMENT "用户性别",
    `last_visit_date` DATETIME REPLACE DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次访问时间",
    `cost` BIGINT SUM DEFAULT "0" COMMENT "用户总消费",
    `max_dwell_time` INT MAX DEFAULT "0" COMMENT "用户最大停留时间",
    `min_dwell_time` INT MIN DEFAULT "99999" COMMENT "用户最小停留时间"
)
AGGREGATE KEY(`user_id`, `date`, `timestamp` ,`city`, `age`, `sex`)
DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1"
);
```

Suppose that the import data are as follows:

| user_id | date       | timestamp           | city      | age | sex | last\_visit\_date   | cost | max\_dwell\_time | min\_dwell\_time |
|---------|------------|---------------------|-----------|-----|-----|---------------------|------|------------------|------------------|
| 10000   | 2017-10-01 | 2017-10-01 08:00:05 | Beijing   | 20  | 0   | 2017-10-01 06:00    | 20   | 10               | 10               |
| 10000   | 2017-10-01 | 2017-10-01 09:00:05 | Beijing   | 20  | 0   | 2017-10-01 07:00    | 15   | 2                | 2                |
| 10001   | 2017-10-01 | 2017-10-01 18:12:10 | Beijing   | 30  | 1   | 2017-10-01 17:05:45 | 2    | 22               | 22               |
| 10002   | 2017-10-02 | 2017-10-02 13:10:00 | Shanghai  | 20  | 1   | 2017-10-02 12:59:12 | 200  | 5                | 5                |
| 10003   | 2017-10-02 | 2017-10-02 13:15:00 | Guangzhou | 32  | 0   | 2017-10-02 11:20:00 | 30   | 11               | 11               |
| 10004   | 2017-10-01 | 2017-10-01 12:12:48 | Shenzhen  | 35  | 0   | 2017-10-01 10:00:15 | 100  | 3                | 3                |
| 10004   | 2017-10-03 | 2017-10-03 12:38:20 | Shenzhen  | 35  | 0   | 2017-10-03 10:20:22 | 11   | 6                | 6                |

And you can import data with the following sql:

```sql
insert into example_db.example_tbl_agg2 values
(10000,"2017-10-01","2017-10-01 08:00:05","Beijing",20,0,"2017-10-01 06:00:00",20,10,10),
(10000,"2017-10-01","2017-10-01 09:00:05","Beijing",20,0,"2017-10-01 07:00:00",15,2,2),
(10001,"2017-10-01","2017-10-01 18:12:10","Beijing",30,1,"2017-10-01 17:05:45",2,22,22),
(10002,"2017-10-02","2017-10-02 13:10:00","Shanghai",20,1,"2017-10-02 12:59:12",200,5,5),
(10003,"2017-10-02","2017-10-02 13:15:00","Guangzhou",32,0,"2017-10-02 11:20:00",30,11,11),
(10004,"2017-10-01","2017-10-01 12:12:48","Shenzhen",35,0,"2017-10-01 10:00:15",100,3,3),
(10004,"2017-10-03","2017-10-03 12:38:20","Shenzhen",35,0,"2017-10-03 10:20:22",11,6,6);
```

After importing, this batch of data will be stored in Doris as follows:

| user_id | date       | timestamp           | city      | age | sex | last\_visit\_date   | cost | max\_dwell\_time | min\_dwell\_time |
|---------|------------|---------------------|-----------|-----|-----|---------------------|------|------------------|------------------|
| 10000   | 2017-10-01 | 2017-10-01 08:00:05 | Beijing   | 20  | 0   | 2017-10-01 06:00    | 20   | 10               | 10               |
| 10000   | 2017-10-01 | 2017-10-01 09:00:05 | Beijing   | 20  | 0   | 2017-10-01 07:00    | 15   | 2                | 2                |
| 10001   | 2017-10-01 | 2017-10-01 18:12:10 | Beijing   | 30  | 1   | 2017-10-01 17:05:45 | 2    | 22               | 22               |
| 10002   | 2017-10-02 | 2017-10-02 13:10:00 | Shanghai  | 20  | 1   | 2017-10-02 12:59:12 | 200  | 5                | 5                |
| 10003   | 2017-10-02 | 2017-10-02 13:15:00 | Guangzhou | 32  | 0   | 2017-10-02 11:20:00 | 30   | 11               | 11               |
| 10004   | 2017-10-01 | 2017-10-01 12:12:48 | Shenzhen  | 35  | 0   | 2017-10-01 10:00:15 | 100  | 3                | 3                |
| 10004   | 2017-10-03 | 2017-10-03 12:38:20 | Shenzhen  | 35  | 0   | 2017-10-03 10:20:22 | 11   | 6                | 6                |

As you can see, the stored data are exactly the same as the import data. No aggregation has ever happened. This is because, the newly added `timestamp` column results in **difference of Keys** among the rows. That is to say, as long as the Keys of the rows are not identical in the import data, Doris can save the complete detailed data even in the Aggregate Model.

### Example 3: Aggregate Import Data and Existing Data

Based on Example 1, suppose that you have the following data stored in Doris:

| user_id | date       | city      | age | sex | last\_visit\_date   | cost | max\_dwell\_time | min\_dwell\_time |
|---------|------------|-----------|-----|-----|---------------------|------|------------------|------------------|
| 10000   | 2017-10-01 | Beijing   | 20  | 0   | 2017-10-01 07:00    | 35   | 10               | 2                |
| 10001   | 2017-10-01 | Beijing   | 30  | 1   | 2017-10-01 17:05:45 | 2    | 22               | 22               |
| 10002   | 2017-10-02 | Shanghai  | 20  | 1   | 2017-10-02 12:59:12 | 200  | 5                | 5                |
| 10003   | 2017-10-02 | Guangzhou | 32  | 0   | 2017-10-02 11:20:00 | 30   | 11               | 11               |
| 10004   | 2017-10-01 | Shenzhen  | 35  | 0   | 2017-10-01 10:00:15 | 100  | 3                | 3                |
| 10004   | 2017-10-03 | Shenzhen  | 35  | 0   | 2017-10-03 10:20:22 | 11   | 6                | 6                |

Now you need to import a new batch of data:

| user_id | date       | city     | age | sex | last\_visit\_date   | cost | max\_dwell\_time | min\_dwell\_time |
|---------|------------|----------|-----|-----|---------------------|------|------------------|------------------|
| 10004   | 2017-10-03 | Shenzhen | 35  | 0   | 2017-10-03 11:22:00 | 44   | 19               | 19               |
| 10005   | 2017-10-03 | Changsha | 29  | 1   | 2017-10-03 18:11:02 | 3    | 1                | 1                |

And you can import data with the following sql:

```sql
insert into example_db.example_tbl_agg1 values
(10004,"2017-10-03","Shenzhen",35,0,"2017-10-03 11:22:00",44,19,19),
(10005,"2017-10-03","Changsha",29,1,"2017-10-03 18:11:02",3,1,1);
```

After importing, the data stored in Doris will be updated as follows:

| user_id | date       | city      | age | sex | last\_visit\_date   | cost | max\_dwell\_time | min\_dwell\_time |
|---------|------------|-----------|-----|-----|---------------------|------|------------------|------------------|
| 10000   | 2017-10-01 | Beijing   | 20  | 0   | 2017-10-01 07:00    | 35   | 10               | 2                |
| 10001   | 2017-10-01 | Beijing   | 30  | 1   | 2017-10-01 17:05:45 | 2    | 22               | 22               |
| 10002   | 2017-10-02 | Shanghai  | 20  | 1   | 2017-10-02 12:59:12 | 200  | 5                | 5                |
| 10003   | 2017-10-02 | Guangzhou | 32  | 0   | 2017-10-02 11:20:00 | 30   | 11               | 11               |
| 10004   | 2017-10-01 | Shenzhen  | 35  | 0   | 2017-10-01 10:00:15 | 100  | 3                | 3                |
| 10004   | 2017-10-03 | Shenzhen  | 35  | 0   | 2017-10-03 11:22:00 | 55   | 19               | 6                |
| 10005   | 2017-10-03 | Changsha  | 29  | 1   | 2017-10-03 18:11:02 | 3    | 1                | 1                |

As you can see, the existing data and the newly imported data of User 10004 have been aggregated. Meanwhile, the new data of User 10005 have been added.

In Doris, data aggregation happens in the following 3 stages:

1. The ETL stage of each batch of import data. At this stage, the batch of import data will be aggregated internally.
2. The data compaction stage of the underlying BE. At this stage, BE will aggregate data from different batches that have been imported.
3. The data query stage. The data involved in the query will be aggregated accordingly.

At different stages, data will be aggregated to varying degrees. For example, when a batch of data is just imported, it may not be aggregated with the existing data. But for users, they **can only query aggregated data**. That is, what users see are the aggregated data, and they **should not assume that what they have seen are not or partly aggregated**. (See the [Limitations of Aggregate Model](#Limitations of Aggregate Model) section for more details.)

### agg_state

    AGG_STATE cannot be used as a key column, and when creating a table, you need to declare the signature of the aggregation function. Users do not need to specify a length or default value. The actual storage size of the data depends on the function implementation.

CREATE TABLE

```sql
set enable_agg_state=true;
create table aggstate(
    k1 int null,
    k2 agg_state sum(int),
    k3 agg_state group_concat(string)
)
aggregate key (k1)
distributed BY hash(k1) buckets 3
properties("replication_num" = "1");
```


"agg_state" is used to declare the data type as "agg_state," and "sum/group_concat" are the signatures of aggregation functions.

Please note that "agg_state" is a data type, similar to "int," "array," or "string."

"agg_state" can only be used in conjunction with the [state](../sql-manual/sql-functions/combinators/state.md)/[merge](../sql-manual/sql-functions/combinators/merge.md)/[union](../sql-manual/sql-functions/combinators/union.md) function combinators.

"agg_state" represents an intermediate result of an aggregation function. For example, with the aggregation function "sum," "agg_state" can represent the intermediate state of summing values like sum(1, 2, 3, 4, 5), rather than the final result.

The "agg_state" type needs to be generated using the "state" function. For the current table, it would be "sum_state" and "group_concat_state" for the "sum" and "group_concat" aggregation functions, respectively.

```sql
insert into aggstate values(1,sum_state(1),group_concat_state('a'));
insert into aggstate values(1,sum_state(2),group_concat_state('b'));
insert into aggstate values(1,sum_state(3),group_concat_state('c'));
```

At this point, the table contains only one row. Please note that the table below is for illustrative purposes and cannot be selected/displayed directly:

| k1 | k2         | k3                        |               
|----|------------|---------------------------| 
| 1  | sum(1,2,3) | group_concat_state(a,b,c) | 

Insert another record.

```sql
insert into aggstate values(2,sum_state(4),group_concat_state('d'));
```

The table's structure at this moment is...

| k1 | k2         | k3                        |               
|----|------------|---------------------------| 
| 1  | sum(1,2,3) | group_concat_state(a,b,c) | 
| 2  | sum(4)     | group_concat_state(d)     |

We can use the 'merge' operation to combine multiple states and return the final result calculated by the aggregation function.

```
mysql> select sum_merge(k2) from aggstate;
+---------------+
| sum_merge(k2) |
+---------------+
|            10 |
+---------------+
```

`sum_merge` will first combine sum(1,2,3) and sum(4) into sum(1,2,3,4), and return the calculated result.
Because `group_concat` has a specific order requirement, the result is not stable.

```
mysql> select group_concat_merge(k3) from aggstate;
+------------------------+
| group_concat_merge(k3) |
+------------------------+
| c,b,a,d                |
+------------------------+
```

If you do not want the final aggregation result, you can use 'union' to combine multiple intermediate aggregation results and generate a new intermediate result.

```sql
insert into aggstate select 3,sum_union(k2),group_concat_union(k3) from aggstate ;
```

The table's structure at this moment is...

| k1 | k2           | k3                          |               
|----|--------------|-----------------------------| 
| 1  | sum(1,2,3)   | group_concat_state(a,b,c)   | 
| 2  | sum(4)       | group_concat_state(d)       |
| 3  | sum(1,2,3,4) | group_concat_state(a,b,c,d) |

You can achieve this through a query.

```
mysql> select sum_merge(k2) , group_concat_merge(k3)from aggstate;
+---------------+------------------------+
| sum_merge(k2) | group_concat_merge(k3) |
+---------------+------------------------+
|            20 | c,b,a,d,c,b,a,d        |
+---------------+------------------------+

mysql> select sum_merge(k2) , group_concat_merge(k3)from aggstate where k1 != 2;
+---------------+------------------------+
| sum_merge(k2) | group_concat_merge(k3) |
+---------------+------------------------+
|            16 | c,b,a,d,c,b,a          |
+---------------+------------------------+
```

Users can perform more detailed aggregation function operations using `agg_state`.

Please note that `agg_state` comes with a certain performance overhead.

## Unique Model

In some multidimensional analysis scenarios, users are highly concerned about how to ensure the uniqueness of the Key,
that is, how to create uniqueness constraints for the Primary Key. Therefore, we introduce the Unique Model. Prior to Doris 1.2,
the Unique Model was essentially a special case of the Aggregate Model and a simplified representation of table schema.
The Aggregate Model is implemented by Merge on Read, so it might not deliver high performance in some aggregation queries
(see the [Limitations of Aggregate Model](#limitations-of-aggregate-model) section). In Doris 1.2, 
we have introduced a new implementation for the Unique Model--Merge on Write, which can help achieve optimal query performance.
For now, Merge on Read and Merge on Write will coexist in the Unique Model for a while, but in the future, 
we plan to make Merge on Write the default implementation of the Unique Model. The following will illustrate the two implementations with examples.

### Merge on Read ( Same Implementation as Aggregate Model)

| ColumnName    | Type          | IsKey | Comment                |
|---------------|---------------|-------|------------------------|
| user_id       | BIGINT        | Yes   | User ID                |
| username      | VARCHAR (50)  | Yes   | Username               |
| city          | VARCHAR (20)  | No    | User location city     |
| age           | SMALLINT      | No    | User age               |
| sex           | TINYINT       | No    | User gender            |
| phone         | LARGEINT      | No    | User phone number      |
| address       | VARCHAR (500) | No    | User address           |
| register_time | DATETIME      | No    | User registration time |

This is a typical user basic information table. There is no aggregation requirement for such data. The only concern is to ensure the uniqueness of the primary key. (The primary key here is user_id + username). The CREATE TABLE statement for the above table is as follows:

```
CREATE TABLE IF NOT EXISTS example_db.example_tbl_unique
(
`user_id` LARGEINT NOT NULL COMMENT "User ID",
`username` VARCHAR (50) NOT NULL COMMENT "Username",
`city` VARCHAR (20) COMMENT "User location city",
`age` SMALLINT COMMENT "User age",
`sex` TINYINT COMMENT "User sex",
`phone` LARGEINT COMMENT "User phone number",
`address` VARCHAR (500) COMMENT "User address",
`register_time` DATETIME COMMENT "User registration time"
)
UNIQUE KEY (`user_id`, `username`)
DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1"
);
```

This is the same table schema and the CREATE TABLE statement as those of the Aggregate Model:

| ColumnName    | Type          | AggregationType | Comment                |
|---------------|---------------|-----------------|------------------------|
| user_id       | BIGINT        |                 | User ID                |
| username      | VARCHAR (50)  |                 | Username               |
| city          | VARCHAR (20)  | REPLACE         | User location city     |
| age           | SMALLINT      | REPLACE         | User age               |
| sex           | TINYINT       | REPLACE         | User gender            |
| phone         | LARGEINT      | REPLACE         | User phone number      |
| address       | VARCHAR (500) | REPLACE         | User address           |
| register_time | DATETIME      | REPLACE         | User registration time |

```
CREATE TABLE IF NOT EXISTS example_db.example_tbl_agg3
(
`user_id` LARGEINT NOT NULL COMMENT "User ID",
`username` VARCHAR (50) NOT NULL COMMENT "Username",
`city` VARCHAR (20) REPLACE COMMENT "User location city",
`sex` TINYINT REPLACE COMMENT "User gender",
`phone` LARGEINT REPLACE COMMENT "User phone number",
`address` VARCHAR(500) REPLACE COMMENT "User address",
`register_time` DATETIME REPLACE COMMENT "User registration time"
)
AGGREGATE KEY(`user_id`, `username`)
DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1"
);
```

That is to say, the Merge on Read implementation of the Unique Model is equivalent to the REPLACE aggregation type in the Aggregate Model. The internal implementation and data storage are exactly the same.

<version since="1.2">

### Merge on Write

The Merge on Write implementation of the Unique Model is completely different from that of the Aggregate Model. It can deliver better performance in aggregation queries with primary key limitations.

In Doris 1.2.0, as a new feature, Merge on Write is disabled by default, and users can enable it by adding the following property:

```
"enable_unique_key_merge_on_write" = "true"
```

> NOTE:
> 1. It is recommended to use version 1.2.4 or above, as this version has fixed some bugs and stability issues.
> 2. Add the configuration item "disable_storage_page_cache=false" to the be.conf file. Failure to add this configuration item may have a significant impact on data load performance.

Take the previous table as an example, the corresponding to CREATE TABLE statement should be:

```
CREATE TABLE IF NOT EXISTS example_db.example_tbl_unique_merge_on_write
(
`user_id` LARGEINT NOT NULL COMMENT "User ID",
`username` VARCHAR (50) NOT NULL COMMENT "Username",
`city` VARCHAR (20) COMMENT "User location city",
`age` SMALLINT COMMENT "Userage",
`sex` TINYINT COMMENT "User gender",
`phone` LARGEINT COMMENT "User phone number",
`address` VARCHAR (500) COMMENT "User address",
`register_time` DATETIME COMMENT "User registration time"
)
UNIQUE KEY (`user_id`, `username`)
DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1"
"enable_unique_key_merge_on_write" = "true"
);
```

The table schema produced by the above statement will be different from that of the Aggregate Model.


| ColumnName    | Type          | AggregationType | Comment                |
|---------------|---------------|-----------------|------------------------|
| user_id       | BIGINT        |                 | User ID                |
| username      | VARCHAR (50)  |                 | Username               |
| city          | VARCHAR (20)  | NONE            | User location city     |
| age           | SMALLINT      | NONE            | User age               |
| sex           | TINYINT       | NONE            | User gender            |
| phone         | LARGEINT      | NONE            | User phone number      |
| address       | VARCHAR (500) | NONE            | User address           |
| register_time | DATETIME      | NONE            | User registration time |

On a Unique table with the Merge on Write option enabled, during the import stage, the data that are to be overwritten and updated will be marked for deletion, and new data will be written in. When querying, all data marked for deletion will be filtered out at the file level, and only the latest data would be readed. This eliminates the data aggregation cost while reading, and supports many types of predicate pushdown now. Therefore, it can largely improve performance in many scenarios, especially in aggregation queries.

[NOTE]

1. The new Merge on Write implementation is disabled by default, and can only be enabled by specifying a property when creating a new table.
2. The old Merge on Read cannot be seamlessly upgraded to the new implementation (since they have completely different data organization). If you want to switch to the Merge on Write implementation, you need to manually execute `insert into unique-mow- table select * from source table` to load data to new table.
3. The two unique features `delete sign` and `sequence col` of the Unique Model can be used as normal in the new implementation, and their usage remains unchanged.

</version>

## Duplicate Model

In some multidimensional analysis scenarios, there is no need for primary keys or data aggregation. For these cases, we introduce the Duplicate Model to. Here is an example:

| ColumnName | Type           | SortKey | Comment        |
|------------|----------------|---------|----------------|
| timstamp   | DATETIME       | Yes     | Log time       |
| type       | INT            | Yes     | Log type       |
| error_code | INT            | Yes     | Error code     |
| Error_msg  | VARCHAR (1024) | No      | Error details  |
| op_id      | BIGINT         | No      | Operator ID    |
| op_time    | DATETIME       | No      | Operation time |

The corresponding to CREATE TABLE statement is as follows:

```
CREATE TABLE IF NOT EXISTS example_db.example_tbl_duplicate
(
    `timestamp` DATETIME NOT NULL COMMENT "Log time",
    `type` INT NOT NULL COMMENT "Log type",
    `error_code` INT COMMENT "Error code",
    `error_msg` VARCHAR(1024) COMMENT "Error details",
    `op_id` BIGINT COMMENT "Operator ID",
    `op_time` DATETIME COMMENT "Operation time"
)
DUPLICATE KEY(`timestamp`, `type`, `error_code`)
DISTRIBUTED BY HASH(`type`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1"
);
```

Different from the Aggregate and Unique Models, the Duplicate Model stores the data as they are and executes no aggregation. Even if there are two identical rows of data, they will both be retained.
The DUPLICATE KEY in the CREATE TABLE statement is only used to specify based on which columns the data are sorted.
(A more appropriate name than DUPLICATE KEY would be SORTING COLUMN, but it is named as such to specify the data model used. For more information,
see [Prefix Index](./index/index-overview.md).) For the choice of DUPLICATE KEY, we recommend the first 2-4 columns.

The Duplicate Model is suitable for storing raw data without aggregation requirements or primary key uniqueness constraints.
For more usage scenarios, see the [Limitations of Aggregate Model](#limitations-of-aggregate-model) section.

### Duplicate Model Without SORTING COLUMN (Since Doris 2.0)

When creating a table without specifying Unique, Aggregate, or Duplicate, a table with a Duplicate model will be created by default, and the SORTING COLUMN will be automatically specified.

When users do not need SORTING COLUMN or Prefix Index, they can configure the following table property:

```
"enable_duplicate_without_keys_by_default" = "true"
```

Then, when creating the default model, the sorting column will no longer be specified, and no prefix index will be created for the table to reduce additional overhead in importing and storing.

The corresponding to CREATE TABLE statement is as follows:

```sql
CREATE TABLE IF NOT EXISTS example_db.example_tbl_duplicate_without_keys_by_default
(
    `timestamp` DATETIME NOT NULL COMMENT "日志时间",
    `type` INT NOT NULL COMMENT "日志类型",
    `error_code` INT COMMENT "错误码",
    `error_msg` VARCHAR(1024) COMMENT "错误详细信息",
    `op_id` BIGINT COMMENT "负责人id",
    `op_time` DATETIME COMMENT "处理时间"
)
DISTRIBUTED BY HASH(`type`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"enable_duplicate_without_keys_by_default" = "true"
);

MySQL > desc example_tbl_duplicate_without_keys_by_default;
+------------+---------------+------+-------+---------+-------+
| Field      | Type          | Null | Key   | Default | Extra |
+------------+---------------+------+-------+---------+-------+
| timestamp  | DATETIME      | No   | false | NULL    | NONE  |
| type       | INT           | No   | false | NULL    | NONE  |
| error_code | INT           | Yes  | false | NULL    | NONE  |
| error_msg  | VARCHAR(1024) | Yes  | false | NULL    | NONE  |
| op_id      | BIGINT        | Yes  | false | NULL    | NONE  |
| op_time    | DATETIME      | Yes  | false | NULL    | NONE  |
+------------+---------------+------+-------+---------+-------+
6 rows in set (0.01 sec)
```

## Limitations of Aggregate Model

This section is about the limitations of the Aggregate Model.

The Aggregate Model only presents the aggregated data. That means we have to ensure the presentation consistency of data that has not yet been aggregated (for example, two different import batches). The following provides further explanation with examples.

Suppose that you have the following table schema:

| ColumnName | Type     | AggregationType | Comment                         |
|------------|----------|-----------------|---------------------------------|
| user\_id   | LARGEINT |                 | User ID                         |
| date       | DATE     |                 | Date when the data are imported |
| cost       | BIGINT   | SUM             | Total user consumption          |

Assume that there are two batches of data that have been imported into the storage engine as follows:

**batch 1**

| user\_id | date       | cost |
|----------|------------|------|
| 10001    | 2017-11-20 | 50   |
| 10002    | 2017-11-21 | 39   |

**batch 2**

| user\_id | date       | cost |
|----------|------------|------|
| 10001    | 2017-11-20 | 1    |
| 10001    | 2017-11-21 | 5    |
| 10003    | 2017-11-22 | 22   |

As you can see, data about User 10001 in these two import batches have not yet been aggregated. However, in order to ensure that users can only query the aggregated data as follows:

| user\_id | date       | cost |
|----------|------------|------|
| 10001    | 2017-11-20 | 51   |
| 10001    | 2017-11-21 | 5    |
| 10002    | 2017-11-21 | 39   |
| 10003    | 2017-11-22 | 22   |

We have added an aggregation operator to the  query engine to ensure the presentation consistency of data.

In addition, on the aggregate column (Value), when executing aggregate class queries that are inconsistent with the aggregate type, please pay attention to the semantics. For example, in the example above, if you execute the following query:

`SELECT MIN(cost) FROM table;`

The result will be 5, not 1.

Meanwhile, this consistency guarantee could considerably reduce efficiency in some queries.

Take the basic count (*) query as an example:

`SELECT COUNT(*) FROM table;`

In other databases, such queries return results quickly. Because in actual implementation, the models can get the query result by counting rows and saving the statistics upon import, or by scanning only one certain column of data to get count value upon query, with very little overhead. But in Doris's Aggregation Model, the overhead of such queries is **large**.

For the previous example:

**batch 1**

| user\_id | date       | cost |
|----------|------------|------|
| 10001    | 2017-11-20 | 50   |
| 10002    | 2017-11-21 | 39   |

**batch 2**

| user\_id | date       | cost |
|----------|------------|------|
| 10001    | 2017-11-20 | 1    |
| 10001    | 2017-11-21 | 5    |
| 10003    | 2017-11-22 | 22   |

Since the final aggregation result is:

| user\_id | date       | cost |
|----------|------------|------|
| 10001    | 2017-11-20 | 51   |
| 10001    | 2017-11-21 | 5    |
| 10002    | 2017-11-21 | 39   |
| 10003    | 2017-11-22 | 22   |

The correct result of  `select count (*) from table;`  should be **4**. But if the model only scans the `user_id` 
column and operates aggregation upon query, the final result will be **3** (10001, 10002, 10003). 
And if it does not operate aggregation, the final result will be **5** (a total of five rows in two batches). Apparently, both results are wrong.

In order to get the correct result, we must read both the  `user_id` and `date` column, and **performs aggregation** when querying.
That is to say, in the `count (*)` query, Doris must scan all AGGREGATE KEY columns (in this case, `user_id` and `date`) 
and aggregate them to get the semantically correct results. That means if there are many aggregated columns, `count (*)` queries could involve scanning large amounts of data.

Therefore, if you need to perform frequent `count (*)` queries, we recommend that you simulate `count (*)` by adding a 
column of value 1 and aggregation type SUM. In this way, the table schema in the previous example will be modified as follows:

| ColumnName | Type   | AggregationType | Comment                         |
|------------|--------|-----------------|---------------------------------|
| user ID    | BIGINT |                 | User ID                         |
| date       | DATE   |                 | Date when the data are imported |
| Cost       | BIGINT | SUM             | Total user consumption          |
| count      | BIGINT | SUM             | For count queries               |

The above adds a count column, the value of which will always be **1**, so the result of `select count (*) from table;`
is equivalent to that of `select sum (count) from table;` The latter is much more efficient than the former. However,
this method has its shortcomings, too. That is, it  requires that users will not import rows with the same values in the
AGGREGATE KEY columns. Otherwise, `select sum (count) from table;` can only express the number of rows of the originally imported data, instead of the semantics of `select count (*) from table;`

Another method is to add a `cound` column of value 1 but aggregation type of REPLACE. Then `select sum (count) from table;`
and `select count (*) from table;`  could produce the same results. Moreover, this method does not require the absence of same AGGREGATE KEY columns in the import data.

### Merge on Write of Unique Model

The Merge on Write implementation in the Unique Model does not impose the same limitation as the Aggregate Model. 
In Merge on Write, the model adds a  `delete bitmap` for each imported rowset to mark the data being overwritten or deleted. With the previous example, after Batch 1 is imported, the data status will be as follows:

**batch 1**

| user_id | date       | cost | delete bit |
|---------|------------|------|------------|
| 10001   | 2017-11-20 | 50   | false      |
| 10002   | 2017-11-21 | 39   | false      |

After Batch 2 is imported, the duplicate rows in the first batch will be marked as deleted, and the status of the two batches of data is as follows

**batch 1**

| user_id | date       | cost | delete bit |
|---------|------------|------|------------|
| 10001   | 2017-11-20 | 50   | **true**   |
| 10002   | 2017-11-21 | 39   | false      |

**batch 2**

| user\_id | date       | cost | delete bit |
|----------|------------|------|------------|
| 10001    | 2017-11-20 | 1    | false      |
| 10001    | 2017-11-21 | 5    | false      |
| 10003    | 2017-11-22 | 22   | false      |

In queries, all data marked `true` in the `delete bitmap` will not be read, so there is no need for data aggregation.
Since there are 4 valid rows in the above data, the query result should also be 4.  This also enables minimum overhead since it only scans one column of data.

In the test environment, `count(*)` queries in Merge on Write of the Unique Model deliver 10 times higher performance than that of the Aggregate Model.

### Duplicate Model

The Duplicate Model does not impose the same limitation as the Aggregate Model because it does not involve aggregation semantics.
For any columns, it can return the semantically correct results in  `count (*)` queries.

## Key Columns

For the Duplicate, Aggregate, and Unique Models, the Key columns will be specified when the table is created,
but there exist some differences: In the Duplicate Model, the Key columns of the table can be regarded as just "sorting columns",
but not unique identifiers. In Aggregate and Unique Models, the Key columns are both "sorting columns" and "unique identifier columns".

## Suggestions for Choosing Data Model

Since the data model was established when the table was built, and **irrevocable thereafter, it is very important to select the appropriate data model**.

1. The Aggregate Model can greatly reduce the amount of data scanned and query computation by pre-aggregation. Thus, it is very suitable for report query scenarios with fixed patterns. But this model is unfriendly to `count (*)` queries. Meanwhile, since the aggregation method on the Value column is fixed, semantic correctness should be considered in other types of aggregation queries.
2. The Unique Model guarantees the uniqueness of primary key for scenarios requiring a unique primary key. The downside is that it cannot exploit the advantage brought by pre-aggregation such as ROLLUP in queries.
   1. Users who have high-performance requirements for aggregate queries are recommended to use the newly added Merge on Write implementation since version 1.2.
   2. The Unique Model only supports entire-row updates. If you require primary key uniqueness as well as partial updates of certain columns (such as loading multiple source tables into one Doris table), you can consider using the Aggregate Model, while setting the aggregate type of the non-primary key columns to REPLACE_IF_NOT_NULL. See [CREATE TABLE Manual](../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE.md) for more details.
3. The Duplicate Model is suitable for ad-hoc queries of any dimensions. Although it may not be able to take advantage of the pre-aggregation feature, it is not limited by what constrains the Aggregate Model and can give full play to the advantage of columnar storage (reading only the relevant columns, but not all Key columns).
