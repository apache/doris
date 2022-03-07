---
{
    "title": "Data Model, ROLLUP and Prefix Index",
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

# Data Model, ROLLUP and Prefix Index

This document describes Doris's data model, ROLLUP and prefix index concepts at the logical level to help users better use Doris to cope with different business scenarios.

## Basic concepts

In Doris, data is logically described in the form of tables.
A table consists of rows and columns. Row is a row of user data. Column is used to describe different fields in a row of data.

Columns can be divided into two categories: Key and Value. From a business perspective, Key and Value can correspond to dimension columns and indicator columns, respectively.

Doris's data model is divided into three main categories:

* Aggregate
* Uniq
* Duplicate

Let's introduce them separately.

## Aggregate Model

We illustrate what aggregation model is and how to use it correctly with practical examples.

### Example 1: Importing data aggregation

Assume that the business has the following data table schema:

|ColumnName|Type|AggregationType|Comment|
|---|---|---|---|
| userid | LARGEINT | | user id|
| date | DATE | | date of data filling|
| City | VARCHAR (20) | | User City|
| age | SMALLINT | | User age|
| sex | TINYINT | | User gender|
| Last_visit_date | DATETIME | REPLACE | Last user access time|
| Cost | BIGINT | SUM | Total User Consumption|
| max dwell time | INT | MAX | Maximum user residence time|
| min dwell time | INT | MIN | User minimum residence time|

If converted into a table-building statement, the following is done (omitting the Partition and Distribution information in the table-building statement)

```
CREATE TABLE IF NOT EXISTS example_db.expamle_tbl
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
... /* ignore Partition and Distribution */
;
```

As you can see, this is a typical fact table of user information and access behavior.
In general star model, user information and access behavior are stored in dimension table and fact table respectively. Here, in order to explain Doris's data model more conveniently, we store the two parts of information in a single table.

The columns in the table are divided into Key (dimension column) and Value (indicator column) according to whether `AggregationType`is set or not. No `AggregationType`, such as `user_id`, `date`, `age`, etc., is set as **Key**, while Aggregation Type is set as **Value**.

When we import data, the same rows and aggregates into one row for the Key column, while the Value column aggregates according to the set `AggregationType`. `AggregationType`currently has the following four ways of aggregation:

1. SUM: Sum, multi-line Value accumulation.
2. REPLACE: Instead, Values in the next batch of data will replace Values in rows previously imported.
3. MAX: Keep the maximum.
4. MIN: Keep the minimum.

Suppose we have the following imported data (raw data):

|user\_id|date|city|age|sex|last\_visit\_date|cost|max\_dwell\_time|min\_dwell\_time|
|---|---|---|---|---|---|---|---|---|
| 10000 | 2017-10-01 | Beijing | 20 | 0 | 2017-10-01 06:00 | 20 | 10 | 10|
| 10000 | 2017-10-01 | Beijing | 20 | 0 | 2017-10-01 07:00 | 15 | 2 | 2|
| 10001 | 2017-10-01 | Beijing | 30 | 1 | 2017-10-01 17:05:45 | 2 | 22 | 22|
| 10002 | 2017-10-02 | Shanghai | 20 | 1 | 2017-10-02 12:59:12 | 200 | 5 | 5|
| 10003 | 2017-10-02 | Guangzhou | 32 | 0 | 2017-10-02 11:20:00 | 30 | 11 | 11|
| 10004 | 2017-10-01 | Shenzhen | 35 | 0 | 2017-10-01 10:00:15 | 100 | 3 | 3|
| 10004 | 2017-10-03 | Shenzhen | 35 | 0 | 2017-10-03 10:20:22 | 11 | 6 | 6|

Let's assume that this is a table that records the user's behavior in accessing a commodity page. Let's take the first row of data as an example and explain it as follows:

| Data | Description|
|---|---|
| 10000 | User id, each user uniquely identifies id|
| 2017-10-01 | Data storage time, accurate to date|
| Beijing | User City|
| 20 | User Age|
| 0 | Gender male (1 for female)|
| 2017-10-01 06:00 | User's time to visit this page, accurate to seconds|
| 20 | Consumption generated by the user's current visit|
| 10 | User's visit, time to stay on the page|
| 10 | User's current visit, time spent on the page (redundancy)|

Then when this batch of data is imported into Doris correctly, the final storage in Doris is is as follows:

|user\_id|date|city|age|sex|last\_visit\_date|cost|max\_dwell\_time|min\_dwell\_time|
|---|---|---|---|---|---|---|---|---|
| 10000 | 2017-10-01 | Beijing | 20 | 0 | 2017-10-01 07:00 | 35 | 10 | 2|
| 10001 | 2017-10-01 | Beijing | 30 | 1 | 2017-10-01 17:05:45 | 2 | 22 | 22|
| 10002 | 2017-10-02 | Shanghai | 20 | 1 | 2017-10-02 12:59:12 | 200 | 5 | 5|
| 10003 | 2017-10-02 | Guangzhou | 32 | 0 | 2017-10-02 11:20:00 | 30 | 11 | 11|
| 10004 | 2017-10-01 | Shenzhen | 35 | 0 | 2017-10-01 10:00:15 | 100 | 3 | 3|
| 10004 | 2017-10-03 | Shenzhen | 35 | 0 | 2017-10-03 10:20:22 | 11 | 6 | 6|

As you can see, there is only one line of aggregated data left for 10,000 users. The data of other users are consistent with the original data. Here we first explain the aggregated data of user 10000:

The first five columns remain unchanged, starting with column 6 `last_visit_date':

*`2017-10-01 07:00`: Because the `last_visit_date`column is aggregated by REPLACE, the `2017-10-01 07:00` column has been replaced by `2017-10-01 06:00'.
> Note: For data in the same import batch, the order of replacement is not guaranteed for the aggregation of REPLACE. For example, in this case, it may be `2017-10-01 06:00'. For data from different imported batches, it can be guaranteed that the data from the latter batch will replace the former batch.

*`35`: Because the aggregation type of the `cost'column is SUM, 35 is accumulated from 20 + 15.
*`10`: Because the aggregation type of the`max_dwell_time'column is MAX, 10 and 2 take the maximum and get 10.
*`2`: Because the aggregation type of `min_dwell_time'column is MIN, 10 and 2 take the minimum value and get 2.

After aggregation, Doris ultimately only stores aggregated data. In other words, detailed data will be lost and users can no longer query the detailed data before aggregation.

### Example 2: Keep detailed data

Following example 1, we modify the table structure as follows:

|ColumnName|Type|AggregationType|Comment|
|---|---|---|---|
| userid | LARGEINT | | user id|
| date | DATE | | date of data filling|
| Time stamp | DATETIME | | Data filling time, accurate to seconds|
| City | VARCHAR (20) | | User City|
| age | SMALLINT | | User age|
| sex | TINYINT | | User gender|
| Last visit date | DATETIME | REPLACE | Last user access time|
| Cost | BIGINT | SUM | Total User Consumption|
| max dwell time | INT | MAX | Maximum user residence time|
| min dwell time | INT | MIN | User minimum residence time|

That is to say, a column of `timestamp` has been added to record the data filling time accurate to seconds.

The imported data are as follows:

|user_id|date|timestamp|city|age|sex|last\_visit\_date|cost|max\_dwell\_time|min\_dwell\_time|
|---|---|---|---|---|---|---|---|---|---|
| 10000 | 2017-10-01 | 2017-10-01 08:00:05 | Beijing | 20 | 0 | 2017-10-01 06:00 | 20 | 10 | 10|
| 10000 | 2017-10-01 | 2017-10-01 09:00:05 | Beijing | 20 | 0 | 2017-10-01 07:00 | 15 | 2 | 2|
| 10001 | 2017-10-01 | 2017-10-01 18:12:10 | Beijing | 30 | 1 | 2017-10-01 17:05:45 | 2 | 22 | 22|
| 10002 | 2017-10-02 | 2017-10-02 13:10:00 | Shanghai | 20 | 1 | 2017-10-02 12:59:12 | 200 | 5 | 5|
| 10003 | 2017-10-02 | 2017-10-02 13:15:00 | Guangzhou | 32 | 0 | 2017-10-02 11:20:00 | 30 | 11 | 11|
| 10004 | 2017-10-01 | 2017-10-01 12:12:48 | Shenzhen | 35 | 0 | 2017-10-01 10:00:15 | 100 | 3 | 3|
| 10004 | 2017-10-03 | 2017-10-03 12:38:20 | Shenzhen | 35 | 0 | 2017-10-03 10:20:22 | 11 | 6 | 6|

Then when this batch of data is imported into Doris correctly, the final storage in Doris is is as follows:

|user_id|date|timestamp|city|age|sex|last\_visit\_date|cost|max\_dwell\_time|min\_dwell\_time|
|---|---|---|---|---|---|---|---|---|---|
| 10000 | 2017-10-01 | 2017-10-01 08:00:05 | Beijing | 20 | 0 | 2017-10-01 06:00 | 20 | 10 | 10|
| 10000 | 2017-10-01 | 2017-10-01 09:00:05 | Beijing | 20 | 0 | 2017-10-01 07:00 | 15 | 2 | 2|
| 10001 | 2017-10-01 | 2017-10-01 18:12:10 | Beijing | 30 | 1 | 2017-10-01 17:05:45 | 2 | 22 | 22|
| 10002 | 2017-10-02 | 2017-10-02 13:10:00 | Shanghai | 20 | 1 | 2017-10-02 12:59:12 | 200 | 5 | 5|
| 10003 | 2017-10-02 | 2017-10-02 13:15:00 | Guangzhou | 32 | 0 | 2017-10-02 11:20:00 | 30 | 11 | 11|
| 10004 | 2017-10-01 | 2017-10-01 12:12:48 | Shenzhen | 35 | 0 | 2017-10-01 10:00:15 | 100 | 3 | 3|
| 10004 | 2017-10-03 | 2017-10-03 12:38:20 | Shenzhen | 35 | 0 | 2017-10-03 10:20:22 | 11 | 6 | 6|

We can see that the stored data, just like the imported data, does not aggregate at all. This is because, in this batch of data, because the `timestamp` column is added, the Keys of all rows are **not exactly the same**. That is, as long as the keys of each row are not identical in the imported data, Doris can save the complete detailed data even in the aggregation model.

### Example 3: Importing data and aggregating existing data

Take Example 1. Suppose that the data in the table are as follows:

|user_id|date|city|age|sex|last\_visit\_date|cost|max\_dwell\_time|min\_dwell\_time|
|---|---|---|---|---|---|---|---|---|
| 10000 | 2017-10-01 | Beijing | 20 | 0 | 2017-10-01 07:00 | 35 | 10 | 2|
| 10001 | 2017-10-01 | Beijing | 30 | 1 | 2017-10-01 17:05:45 | 2 | 22 | 22|
| 10002 | 2017-10-02 | Shanghai | 20 | 1 | 2017-10-02 12:59:12 | 200 | 5 | 5|
| 10003 | 2017-10-02 | Guangzhou | 32 | 0 | 2017-10-02 11:20:00 | 30 | 11 | 11|
| 10004 | 2017-10-01 | Shenzhen | 35 | 0 | 2017-10-01 10:00:15 | 100 | 3 | 3|
| 10004 | 2017-10-03 | Shenzhen | 35 | 0 | 2017-10-03 10:20:22 | 11 | 6 | 6|

We imported a new batch of data:

|user_id|date|city|age|sex|last\_visit\_date|cost|max\_dwell\_time|min\_dwell\_time|
|---|---|---|---|---|---|---|---|---|
| 10004 | 2017-10-03 | Shenzhen | 35 | 0 | 2017-10-03 11:22:00 | 44 | 19 | 19|
| 10005 | 2017-10-03 | Changsha | 29 | 1 | 2017-10-03 18:11:02 | 3 | 1 | 1|

Then when this batch of data is imported into Doris correctly, the final storage in Doris is is as follows:

|user_id|date|city|age|sex|last\_visit\_date|cost|max\_dwell\_time|min\_dwell\_time|
|---|---|---|---|---|---|---|---|---|
| 10000 | 2017-10-01 | Beijing | 20 | 0 | 2017-10-01 07:00 | 35 | 10 | 2|
| 10001 | 2017-10-01 | Beijing | 30 | 1 | 2017-10-01 17:05:45 | 2 | 22 | 22|
| 10002 | 2017-10-02 | Shanghai | 20 | 1 | 2017-10-02 12:59:12 | 200 | 5 | 5|
| 10003 | 2017-10-02 | Guangzhou | 32 | 0 | 2017-10-02 11:20:00 | 30 | 11 | 11|
| 10004 | 2017-10-01 | Shenzhen | 35 | 0 | 2017-10-01 10:00:15 | 100 | 3 | 3|
| 10004 | 2017-10-03 | Shenzhen | 35 | 0 | 2017-10-03 11:22:00 | 55 | 19 | 6|
| 10005 | 2017-10-03 | Changsha | 29 | 1 | 2017-10-03 18:11:02 | 3 | 1 | 1|

As you can see, the existing data and the newly imported data of user 10004 have been aggregated. At the same time, 10005 new user's data were added.

Data aggregation occurs in Doris in the following three stages:

1. The ETL stage of data import for each batch. This phase aggregates data within each batch of imported data.
2. The stage in which the underlying BE performs data Compaction. At this stage, BE aggregates data from different batches that have been imported.
3. Data query stage. In data query, the data involved in the query will be aggregated accordingly.

Data may be aggregated to varying degrees at different times. For example, when a batch of data is just imported, it may not be aggregated with the existing data. But for users, user**can only query aggregated data**. That is, different degrees of aggregation are transparent to user queries. Users should always assume that data exists in terms of the degree of aggregation that **ultimately completes**, and **should not assume that some aggregation has not yet occurred**. (See the section **Limitations of the aggregation model** for more details.)

## Uniq Model

In some multi-dimensional analysis scenarios, users are more concerned with how to ensure the uniqueness of Key, that is, how to obtain the Primary Key uniqueness constraint. Therefore, we introduce Uniq's data model. This model is essentially a special case of aggregation model and a simplified representation of table structure. Let's give an example.

|ColumnName|Type|IsKey|Comment|
|---|---|---|---|
| user_id | BIGINT | Yes | user id|
| username | VARCHAR (50) | Yes | User nickname|
| city | VARCHAR (20) | No | User City|
| age | SMALLINT | No | User Age|
| sex | TINYINT | No | User Gender|
| phone | LARGEINT | No | User Phone|
| address | VARCHAR (500) | No | User Address|
| register_time | DATETIME | No | user registration time|

This is a typical user base information table. There is no aggregation requirement for this type of data, just the uniqueness of the primary key is guaranteed. (The primary key here is user_id + username). Then our statement is as follows:

```
CREATE TABLE IF NOT EXISTS example_db.expamle_tbl
(
`user_id` LARGEINT NOT NULL COMMENT "用户id",
`username` VARCHAR (50) NOT NULL COMMENT "25143;" 261651;"
`city` VARCHAR (20) COMMENT `User City',
`age` SMALLINT COMMENT "29992;" 25143;"24180;" 40836 ",
`sex` TINYINT COMMENT "用户性别",
`phone` LARGEINT COMMENT "用户电话",
`address` VARCHAR (500) COMMENT'25143;',
`register_time` DATETIME COMMENT "29992;" 25143;"27880;" 20876;"26102;" 38388;"
)
Unique Key (`user_id`, `username`)
... /* ignore Partition and Distribution  */
;
```

This table structure is exactly the same as the following table structure described by the aggregation model:

|ColumnName|Type|AggregationType|Comment|
|---|---|---|---|
| user_id | BIGINT | | user id|
| username | VARCHAR (50) | | User nickname|
| City | VARCHAR (20) | REPLACE | User City|
| age | SMALLINT | REPLACE | User Age|
| sex | TINYINT | REPLACE | User Gender|
| Phone | LARGEINT | REPLACE | User Phone|
| address | VARCHAR (500) | REPLACE | User Address|
| register_time | DATETIME | REPLACE | User registration time|

And table-building statements:

```
CREATE TABLE IF NOT EXISTS example_db.expamle_tbl
(
`user_id` LARGEINT NOT NULL COMMENT "用户id",
`username` VARCHAR (50) NOT NULL COMMENT "25143;" 261651;"
`city` VARCHAR (20) REPLACE COMMENT `User City',
`sex` TINYINT REPLACE COMMENT "用户性别",
`phone` LARGEINT REPLACE COMMENT "25143;"
`address` VARCHAR(500) REPLACE COMMENT "用户地址",
`register_time` DATETIME REPLACE COMMENT "29992;" 25143;"27880;" 20876;"26102;"
)
AGGREGATE KEY(`user_id`, `username`)
... /* ignore Partition and Distribution */
;
```

That is to say, Uniq model can be completely replaced by REPLACE in aggregation model. Its internal implementation and data storage are exactly the same. No further examples will be given here.

## Duplicate Model

In some multidimensional analysis scenarios, data has neither primary keys nor aggregation requirements. Therefore, we introduce Duplicate data model to meet this kind of demand. Examples are given.

|ColumnName|Type|SortKey|Comment|
|---|---|---|---|
| Timstamp | DATETIME | Yes | Logging Time|
| Type | INT | Yes | Log Type|
|error_code|INT|Yes|error code|
| Error_msg | VARCHAR (1024) | No | Error Details|
|op_id|BIGINT|No|operator id|
|op_time|DATETIME|No|operation time|

The TABLE statement is as follows:
```
CREATE TABLE IF NOT EXISTS example_db.expamle_tbl
(
`timestamp` DATETIME NOT NULL COMMENT "日志时间",
`type` INT NOT NULL COMMENT "日志类型",
"Error"\\\\\\\\\\\\\
`error_msg` VARCHAR(1024) COMMENT "错误详细信息",
`op_id` BIGINT COMMENT "负责人id",
OP `op `time ` DATETIME COMMENT "22788;" 29702;"26102;" 388;"
)
DUPLICATE KEY(`timestamp`, `type`)
... /* 省略 Partition 和 Distribution 信息 */
;
```

This data model is different from Aggregate and Uniq models. Data is stored entirely in accordance with the data in the imported file, without any aggregation. Even if the two rows of data are identical, they will be retained.
The DUPLICATE KEY specified in the table building statement is only used to specify which columns the underlying data is sorted according to. (The more appropriate name should be "Sorted Column", where the name "DUPLICATE KEY" is used to specify the data model used. For more explanations of "Sorted Column", see the section ** Prefix Index **. On the choice of DUPLICATE KEY, we recommend that the first 2-4 columns be selected appropriately.

This data model is suitable for storing raw data without aggregation requirements and primary key uniqueness constraints. For more usage scenarios, see the ** Limitations of the Aggregation Model ** section.

## ROLLUP

ROLLUP in multidimensional analysis means "scroll up", which means that data is aggregated further at a specified granularity.

### Basic concepts

In Doris, we make the table created by the user through the table building statement a Base table. Base table holds the basic data stored in the way specified by the user's table-building statement.

On top of the Base table, we can create any number of ROLLUP tables. These ROLLUP data are generated based on the Base table and physically **stored independently**.

The basic function of ROLLUP tables is to obtain coarser aggregated data on the basis of Base tables.

Let's illustrate the ROLLUP tables and their roles in different data models with examples.

#### ROLLUP in Aggregate Model and Uniq Model

Because Uniq is only a special case of the Aggregate model, we do not distinguish it here.

Example 1: Get the total consumption per user

Following **Example 2** in the **Aggregate Model** section, the Base table structure is as follows:

|ColumnName|Type|AggregationType|Comment|
|---|---|---|---|
| user_id | LARGEINT | | user id|
| date | DATE | | date of data filling|
| Time stamp | DATETIME | | Data filling time, accurate to seconds|
| City | VARCHAR (20) | | User City|
| age | SMALLINT | | User age|
| sex | TINYINT | | User gender|
| Last_visit_date | DATETIME | REPLACE | Last user access time|
| Cost | BIGINT | SUM | Total User Consumption|
| max dwell time | INT | MAX | Maximum user residence time|
| min dwell time | INT | MIN | User minimum residence time|

The data stored are as follows:

|user_id|date|timestamp|city|age|sex|last\_visit\_date|cost|max\_dwell\_time|min\_dwell\_time|
|---|---|---|---|---|---|---|---|---|---|
| 10000 | 2017-10-01 | 2017-10-01 08:00:05 | Beijing | 20 | 0 | 2017-10-01 06:00 | 20 | 10 | 10|
| 10000 | 2017-10-01 | 2017-10-01 09:00:05 | Beijing | 20 | 0 | 2017-10-01 07:00 | 15 | 2 | 2|
| 10001 | 2017-10-01 | 2017-10-01 18:12:10 | Beijing | 30 | 1 | 2017-10-01 17:05:45 | 2 | 22 | 22|
| 10002 | 2017-10-02 | 2017-10-02 13:10:00 | Shanghai | 20 | 1 | 2017-10-02 12:59:12 | 200 | 5 | 5|
| 10003 | 2017-10-02 | 2017-10-02 13:15:00 | Guangzhou | 32 | 0 | 2017-10-02 11:20:00 | 30 | 11 | 11|
| 10004 | 2017-10-01 | 2017-10-01 12:12:48 | Shenzhen | 35 | 0 | 2017-10-01 10:00:15 | 100 | 3 | 3|
| 10004 | 2017-10-03 | 2017-10-03 12:38:20 | Shenzhen | 35 | 0 | 2017-10-03 10:20:22 | 11 | 6 | 6|

On this basis, we create a ROLLUP:

|ColumnName|
|---|
|user_id|
|cost|

The ROLLUP contains only two columns: user_id and cost. After the creation, the data stored in the ROLLUP is as follows:

|user\_id|cost|
|---|---|
|10000|35|
|10001|2|
|10002|200|
|10003|30|
|10004|111|

As you can see, ROLLUP retains only the results of SUM on the cost column for each user_id. So when we do the following query:

`SELECT user_id, sum(cost) FROM table GROUP BY user_id;`

Doris automatically hits the ROLLUP table, thus completing the aggregated query by scanning only a very small amount of data.

2. Example 2: Get the total consumption, the longest and shortest page residence time of users of different ages in different cities

Follow example 1. Based on the Base table, we create a ROLLUP:

|ColumnName|Type|AggregationType|Comment|
|---|---|---|---|
| City | VARCHAR (20) | | User City|
| age | SMALLINT | | User age|
| Cost | BIGINT | SUM | Total User Consumption|
| max dwell time | INT | MAX | Maximum user residence time|
| min dwell time | INT | MIN | User minimum residence time|

After the creation, the data stored in the ROLLUP is as follows:

|city|age|cost|max\_dwell\_time|min\_dwell\_time|
|---|---|---|---|---|
| Beijing | 20 | 35 | 10 | 2|
| Beijing | 30 | 2 | 22 | 22|
| Shanghai | 20 | 200 | 5 | 5|
| Guangzhou | 32 | 30 | 11 | 11|
| Shenzhen | 35 | 111 | 6 | 3|

When we do the following queries:

* Select City, Age, Sum (Cost), Max (Max dwell time), min (min dwell time) from table group by City, age;*
* `SELECT city, sum(cost), max(max_dwell_time), min(min_dwell_time) FROM table GROUP BY city;`
* `SELECT city, age, sum(cost), min(min_dwell_time) FROM table GROUP BY city, age;`

Doris automatically hits the ROLLUP table.

#### ROLLUP in Duplicate Model

Because the Duplicate model has no aggregate semantics. So the ROLLLUP in this model has lost the meaning of "scroll up". It's just to adjust the column order to hit the prefix index. In the next section, we will introduce prefix index in detail, and how to use ROLLUP to change prefix index in order to achieve better query efficiency.

### Prefix Index and ROLLUP

#### prefix index

Unlike traditional database design, Doris does not support indexing on any column. OLAP databases based on MPP architecture such as Doris usually handle large amounts of data by improving concurrency.
In essence, Doris's data is stored in a data structure similar to SSTable (Sorted String Table). This structure is an ordered data structure, which can be sorted and stored according to the specified column. In this data structure, it is very efficient to search by sorting columns.

In Aggregate, Uniq and Duplicate three data models. The underlying data storage is sorted and stored according to the columns specified in AGGREGATE KEY, UNIQ KEY and DUPLICATE KEY in their respective table-building statements.

The prefix index, which is based on sorting, implements an index method to query data quickly according to a given prefix column.

We use the prefix index of **36 bytes** of a row of data as the prefix index of this row of data. When a VARCHAR type is encountered, the prefix index is truncated directly. We give examples to illustrate:

1. The prefix index of the following table structure is user_id (8 Bytes) + age (4 Bytes) + message (prefix 20 Bytes).

|ColumnName|Type|
|---|---|
|user_id|BIGINT|
|age|INT|
|message|VARCHAR(100)|
|max\_dwell\_time|DATETIME|
|min\_dwell\_time|DATETIME|

2. The prefix index of the following table structure is user_name (20 Bytes). Even if it does not reach 36 bytes, because it encounters VARCHAR, it truncates directly and no longer continues.

|ColumnName|Type|
|---|---|
|user_name|VARCHAR(20)|
|age|INT|
|message|VARCHAR(100)|
|max\_dwell\_time|DATETIME|
|min\_dwell\_time|DATETIME|

When our query condition is the prefix of ** prefix index **, it can greatly speed up the query speed. For example, in the first example, we execute the following queries:

`SELECT * FROM table WHERE user_id=1829239 and age=20;`

The efficiency of this query is much higher than that of ** the following queries:

`SELECT * FROM table WHERE age=20;`

Therefore, when constructing tables, ** correctly choosing column order can greatly improve query efficiency **.

#### ROLLUP adjusts prefix index

Because column order is specified when a table is built, there is only one prefix index for a table. This may be inefficient for queries that use other columns that cannot hit prefix indexes as conditions. Therefore, we can manually adjust the order of columns by creating ROLLUP. Examples are given.

The structure of the Base table is as follows:

|ColumnName|Type|
|---|---|
|user\_id|BIGINT|
|age|INT|
|message|VARCHAR(100)|
|max\_dwell\_time|DATETIME|
|min\_dwell\_time|DATETIME|

On this basis, we can create a ROLLUP table:

|ColumnName|Type|
|---|---|
|age|INT|
|user\_id|BIGINT|
|message|VARCHAR(100)|
|max\_dwell\_time|DATETIME|
|min\_dwell\_time|DATETIME|

As you can see, the columns of ROLLUP and Base tables are exactly the same, just changing the order of user_id and age. So when we do the following query:

`SELECT * FROM table where age=20 and massage LIKE "%error%";`

The ROLLUP table is preferred because the prefix index of ROLLUP matches better.

### Some Explanations of ROLLUP

* The fundamental role of ROLLUP is to improve the query efficiency of some queries (whether by aggregating to reduce the amount of data or by modifying column order to match prefix indexes). Therefore, the meaning of ROLLUP has gone beyond the scope of "roll-up". That's why we named it Materialized Index in the source code.
* ROLLUP is attached to the Base table and can be seen as an auxiliary data structure of the Base table. Users can create or delete ROLLUP based on the Base table, but cannot explicitly specify a query for a ROLLUP in the query. Whether ROLLUP is hit or not is entirely determined by the Doris system.
* ROLLUP data is stored in separate physical storage. Therefore, the more ROLLUP you create, the more disk space you occupy. It also has an impact on the speed of import (the ETL phase of import automatically generates all ROLLUP data), but it does not reduce query efficiency (only better).
* Data updates for ROLLUP are fully synchronized with Base representations. Users need not care about this problem.
* Columns in ROLLUP are aggregated in exactly the same way as Base tables. There is no need to specify or modify ROLLUP when creating it.
* A necessary (inadequate) condition for a query to hit ROLLUP is that all columns ** (including the query condition columns in select list and where) involved in the query exist in the column of the ROLLUP. Otherwise, the query can only hit the Base table.
* Certain types of queries (such as count (*)) cannot hit ROLLUP under any conditions. See the next section **Limitations of the aggregation model**.
* The query execution plan can be obtained by `EXPLAIN your_sql;` command, and in the execution plan, whether ROLLUP has been hit or not can be checked.
* Base tables and all created ROLLUP can be displayed by `DESC tbl_name ALL;` statement.

In this document, you can see [Query how to hit Rollup](hit-the-rollup)

## Limitations of aggregation model

Here we introduce the limitations of Aggregate model (including Uniq model).

In the aggregation model, what the model presents is the aggregated data. That is to say, any data that has not yet been aggregated (for example, two different imported batches) must be presented in some way to ensure consistency. Let's give an example.

The hypothesis table is structured as follows:

|ColumnName|Type|AggregationType|Comment|
|---|---|---|---|
| userid | LARGEINT | | user id|
| date | DATE | | date of data filling|
| Cost | BIGINT | SUM | Total User Consumption|

Assume that there are two batches of data that have been imported into the storage engine as follows:

**batch 1**

|user\_id|date|cost|
|---|---|---|
|10001|2017-11-20|50|
|10002|2017-11-21|39|

**batch 2**

|user\_id|date|cost|
|---|---|---|
|10001|2017-11-20|1|
|10001|2017-11-21|5|
|10003|2017-11-22|22|

As you can see, data belonging to user 10001 in two import batches has not yet been aggregated. However, in order to ensure that users can only query the aggregated data as follows:

|user\_id|date|cost|
|---|---|---|
|10001|2017-11-20|51|
|10001|2017-11-21|5|
|10002|2017-11-21|39|
|10003|2017-11-22|22|

We add aggregation operator to query engine to ensure data consistency.

In addition, on the aggregate column (Value), when executing aggregate class queries that are inconsistent with aggregate types, attention should be paid to semantics. For example, in the example above, we execute the following queries:

`SELECT MIN(cost) FROM table;`

The result is 5, not 1.

At the same time, this consistency guarantee will greatly reduce the query efficiency in some queries.

Let's take the most basic count (*) query as an example:

`SELECT COUNT(*) FROM table;`

In other databases, such queries return results quickly. Because in the implementation, we can get the query result by counting rows at the time of import and saving count statistics information, or by scanning only a column of data to get count value at the time of query, with very little overhead. But in Doris's aggregation model, the overhead of this query ** is very large **.

Let's take the data as an example.

**batch 1**

|user\_id|date|cost|
|---|---|---|
|10001|2017-11-20|50|
|10002|2017-11-21|39|

**batch 2**

|user\_id|date|cost|
|---|---|---|
|10001|2017-11-20|1|
|10001|2017-11-21|5|
|10003|2017-11-22|22|

Because the final aggregation result is:

|user\_id|date|cost|
|---|---|---|
|10001|2017-11-20|51|
|10001|2017-11-21|5|
|10002|2017-11-21|39|
|10003|2017-11-22|22|

So `select count (*) from table;` The correct result should be **4**. But if we only scan the `user_id'column and add query aggregation, the final result is **3** (10001, 10002, 10003). If aggregated without queries, the result is **5** (a total of five rows in two batches). It can be seen that both results are wrong.

In order to get the correct result, we must read the data of `user_id` and `date`, and **together with aggregate** when querying, to return the correct result of **4**. That is to say, in the count (*) query, Doris must scan all AGGREGATE KEY columns (here are `user_id` and `date`) and aggregate them to get the semantically correct results. When aggregated columns are large, count (*) queries need to scan a large amount of data.

Therefore, when there are frequent count (*) queries in the business, we recommend that users simulate count (*) by adding a column with a value of 1 and aggregation type of SUM. As the table structure in the previous example, we modify it as follows:

|ColumnName|Type|AggregationType|Comment|
|---|---|---|---|
| user ID | BIGINT | | user id|
| date | DATE | | date of data filling|
| Cost | BIGINT | SUM | Total User Consumption|
| count | BIGINT | SUM | for counting|

Add a count column and import the data with the column value **equal to 1**. The result of `select count (*) from table;`is equivalent to `select sum (count) from table;` The query efficiency of the latter is much higher than that of the former. However, this method also has limitations, that is, users need to guarantee that they will not import rows with the same AGGREGATE KEY column repeatedly. Otherwise, `select sum (count) from table;`can only express the number of rows originally imported, not the semantics of `select count (*) from table;`

Another way is to **change the aggregation type of the count column above to REPLACE, and still weigh 1**. Then`select sum (count) from table;` and `select count (*) from table;` the results will be consistent. And in this way, there is no restriction on importing duplicate rows.

### Duplicate Model

Duplicate model has no limitation of aggregation model. Because the model does not involve aggregate semantics, when doing count (*) query, we can get the correct semantics by choosing a column of queries arbitrarily.

## Suggestions for Choosing Data Model

Because the data model was established when the table was built, and **could not be modified **. Therefore, it is very important to select an appropriate data model**.

1. Aggregate model can greatly reduce the amount of data scanned and the amount of query computation by pre-aggregation. It is very suitable for report query scenarios with fixed patterns. But this model is not very friendly for count (*) queries. At the same time, because the aggregation method on the Value column is fixed, semantic correctness should be considered in other types of aggregation queries.
2. Uniq model guarantees the uniqueness of primary key for scenarios requiring unique primary key constraints. However, the query advantage brought by pre-aggregation such as ROLLUP cannot be exploited (because the essence is REPLACE, there is no such aggregation as SUM).
3. Duplicate is suitable for ad-hoc queries of any dimension. Although it is also impossible to take advantage of the pre-aggregation feature, it is not constrained by the aggregation model and can take advantage of the queue-store model (only reading related columns, but not all Key columns).
