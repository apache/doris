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

This document describes Doris's data model at the logical level to help users better use Doris to cope with different business scenarios.

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
CREATE TABLE IF NOT EXISTS example_db.example_tbl
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

Then when this batch of data is imported into Doris correctly, the final storage in Doris is as follows:

|user\_id|date|city|age|sex|last\_visit\_date|cost|max\_dwell\_time|min\_dwell\_time|
|---|---|---|---|---|---|---|---|---|
| 10000 | 2017-10-01 | Beijing | 20 | 0 | 2017-10-01 07:00 | 35 | 10 | 2|
| 10001 | 2017-10-01 | Beijing | 30 | 1 | 2017-10-01 17:05:45 | 2 | 22 | 22|
| 10002 | 2017-10-02 | Shanghai | 20 | 1 | 2017-10-02 12:59:12 | 200 | 5 | 5|
| 10003 | 2017-10-02 | Guangzhou | 32 | 0 | 2017-10-02 11:20:00 | 30 | 11 | 11|
| 10004 | 2017-10-01 | Shenzhen | 35 | 0 | 2017-10-01 10:00:15 | 100 | 3 | 3|
| 10004 | 2017-10-03 | Shenzhen | 35 | 0 | 2017-10-03 10:20:22 | 11 | 6 | 6|

As you can see, there is only one line of aggregated data left for 10,000 users. The data of other users are consistent with the original data. Here we first explain the aggregated data of user 10000:

The first five columns remain unchanged, starting with column 6 `last_visit_date`:

*`2017-10-01 07:00`: Because the `last_visit_date`column is aggregated by REPLACE, the `2017-10-01 07:00` column has been replaced by `2017-10-01 06:00`.
> Note: For data in the same import batch, the order of replacement is not guaranteed for the aggregation of REPLACE. For example, in this case, it may be `2017-10-01 06:00`. For data from different imported batches, it can be guaranteed that the data from the latter batch will replace the former batch.

*`35`: Because the aggregation type of the `cost`column is SUM, 35 is accumulated from 20 + 15.
*`10`: Because the aggregation type of the`max_dwell_time`column is MAX, 10 and 2 take the maximum and get 10.
*`2`: Because the aggregation type of `min_dwell_time`column is MIN, 10 and 2 take the minimum value and get 2.

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

Then when this batch of data is imported into Doris correctly, the final storage in Doris is as follows:

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

Then when this batch of data is imported into Doris correctly, the final storage in Doris is as follows:

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

In some multi-dimensional analysis scenarios, users are more concerned about how to ensure the uniqueness of the Key, that is, how to obtain the uniqueness constraint of the Primary Key. Therefore, we introduced the Unique data model. Prior to version 1.2, the model was essentially a special case of the Aggregate Model and a simplified representation of the table structure. The implementation of the aggregation model is merge on read, it has poor performance on some aggregation queries (refer to the description in the subsequent chapter [Limitations of aggregation models] (#Limitations of aggregation models)), In version 1.2, we have introduced a new implementation of the Unique model, merge on write, which achieves optimal query performance by doing some extra work when loading. Merge-on-write will replace merge-on-read as the default implementation of the Unique model in the future, they will coexist for a short period of time. The following will illustrate the two implementation manners with examples.

### Merge on read (same implementation as aggregate model)

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
CREATE TABLE IF NOT EXISTS example_db.example_tbl
(
`user_id` LARGEINT NOT NULL COMMENT "user id",
`username` VARCHAR (50) NOT NULL COMMENT "username",
`city` VARCHAR (20) COMMENT "user city",
`age` SMALLINT COMMENT "age",
`sex` TINYINT COMMENT "sex",
`phone` LARGEINT COMMENT "phone",
`address` VARCHAR (500) COMMENT "address",
`register_time` DATETIME COMMENT "register time"
)
Unique Key (`user_id`, `username`)
DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1"
);
```

This table structure is exactly the same as the following table structure described by the aggregation model:

|ColumnName|Type|AggregationType|Comment|
|---|---|---|---|
| user_id | BIGINT | | user id|
| username | VARCHAR (50) | | User nickname|
| city | VARCHAR (20) | REPLACE | User City|
| age | SMALLINT | REPLACE | User Age|
| sex | TINYINT | REPLACE | User Gender|
| phone | LARGEINT | REPLACE | User Phone|
| address | VARCHAR (500) | REPLACE | User Address|
| register_time | DATETIME | REPLACE | User registration time|

And table-building statements:

```
CREATE TABLE IF NOT EXISTS example_db.example_tbl
(
`user_id` LARGEINT NOT NULL COMMENT "user id",
`username` VARCHAR (50) NOT NULL COMMENT "username",
`city` VARCHAR (20) REPLACE COMMENT "user city",
`sex` TINYINT REPLACE COMMENT "sex",
`phone` LARGEINT REPLACE COMMENT "phone",
`address` VARCHAR(500) REPLACE COMMENT "address",
`register_time` DATETIME REPLACE COMMENT "register time"
)
AGGREGATE KEY(`user_id`, `username`)
DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1"
);
```

That is to say, the merge-on-read implementation of the Unique model can be completely replaced by REPLACE in aggregation model. Its internal implementation and data storage are exactly the same. No further examples will be given here.

### Merge on write (introduced from version 1.2)

The merge-on-write implementation of the Unique model is completely different from the aggregation model. The query performance is closer to the duplicate model. Compared with the aggregation model, it has a better query performance.

In version 1.2, as a new feature, merge-on-write is disabled by default, and users can enable it by adding the following property

```
"enable_unique_key_merge_on_write" = "true"
```

Let's continue to use the previous table as an example, the create table statement:

```
CREATE TABLE IF NOT EXISTS example_db.example_tbl
(
`user_id` LARGEINT NOT NULL COMMENT "user id",
`username` VARCHAR (50) NOT NULL COMMENT "username",
`city` VARCHAR (20) COMMENT "user city",
`age` SMALLINT COMMENT "age",
`sex` TINYINT COMMENT "sex",
`phone` LARGEINT COMMENT "phone",
`address` VARCHAR (500) COMMENT "address",
`register_time` DATETIME COMMENT "register time"
)
Unique Key (`user_id`, `username`)
DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1"
"enable_unique_key_merge_on_write" = "true"
);
```

In this implementation, the table structure is completely different with aggregate model:


|ColumnName|Type|AggregationType|Comment|
|---|---|---|---|
| user_id | BIGINT | | user id|
| username | VARCHAR (50) | | User nickname|
| city | VARCHAR (20) | NONE | User City|
| age | SMALLINT | NONE | User Age|
| sex | TINYINT | NONE | User Gender|
| phone | LARGEINT | NONE | User Phone|
| address | VARCHAR (500) | NONE | User Address|
| register_time | DATETIME | NONE | User registration time|

On a Unique table with the merge-on-write option enabled, the data that is overwritten and updated will be marked for deletion during the load job, and new data will be written to a new file at the same time. When querying, all data marked for deletion will be filtered out at the file level, only the latest data would be readed, which eliminates the data aggregation cost while reading, and can support many predicates pushdown now. Therefore, it can bring a relatively large performance improvement in many scenarios, especially in the case of aggregation queries.

[NOTE]
1. The new Merge-on-write implementation is disabled by default, and can only be enabled by specifying a property when creating a new table.
2. The old Merge-on-read implementation cannot be seamlessly upgraded to the new version (the data organization is completely different). If you need to use the merge-on-write implementation version, you need to manually execute `insert into unique-mow- table select * from source table` to load data to new table.
3. The feature delete sign and sequence col on the Unique model can still be used normally in the new implementation, and the usage has not changed.

## Duplicate Model

In some multidimensional analysis scenarios, data has neither primary keys nor aggregation requirements. Therefore, we introduce Duplicate data model to meet this kind of demand. Examples are given.

|ColumnName|Type|SortKey|Comment|
|---|---|---|---|
| timstamp | DATETIME | Yes | Logging Time|
| type | INT | Yes | Log Type|
| error_code|INT|Yes|error code|
| Error_msg | VARCHAR (1024) | No | Error Details|
| op_id|BIGINT|No|operator id|
| op_time|DATETIME|No|operation time|

The TABLE statement is as follows:
```
CREATE TABLE IF NOT EXISTS example_db.example_tbl
(
    `timestamp` DATETIME NOT NULL COMMENT "log time",
    `type` INT NOT NULL COMMENT "log type",
    `error_code` INT COMMENT "error code",
    `error_msg` VARCHAR(1024) COMMENT "error detail",
    `op_id` BIGINT COMMENT "operater id",
    `op_time` DATETIME COMMENT "operate time"
)
DUPLICATE KEY(`timestamp`, `type`, `error_code`)
DISTRIBUTED BY HASH(`type`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1"
);
```

This data model is different from Aggregate and Uniq models. Data is stored entirely in accordance with the data in the imported file, without any aggregation. Even if the two rows of data are identical, they will be retained.
The DUPLICATE KEY specified in the table building statement is only used to specify which columns the underlying data is sorted according to. (The more appropriate name should be "Sorted Column", where the name "DUPLICATE KEY" is used to specify the data model used. For more explanations of "Sorted Column", see the section **Prefix Index**.) On the choice of DUPLICATE KEY, we recommend that the first 2-4 columns be selected appropriately.

This data model is suitable for storing raw data without aggregation requirements and primary key uniqueness constraints. For more usage scenarios, see the **Limitations of the Aggregation Model** section.

## Limitations of aggregation model

Here we introduce the limitations of Aggregate model.

In the aggregation model, what the model presents is the aggregated data. That is to say, any data that has not yet been aggregated (for example, two different imported batches) must be presented in some way to ensure consistency. Let's give an example.

The hypothesis table is structured as follows:

|ColumnName|Type|AggregationType|Comment|
|---|---|---|---|
| user\_id | LARGEINT | | user id|
| date | DATE | | date of data filling|
| cost | BIGINT | SUM | Total User Consumption|

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

In other databases, such queries return results quickly. Because in the implementation, we can get the query result by counting rows at the time of import and saving count statistics information, or by scanning only a column of data to get count value at the time of query, with very little overhead. But in Doris's aggregation model, the overhead of this query is **very large**.

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

So `select count (*) from table;` The correct result should be **4**. But if we only scan the `user_id`column and add query aggregation, the final result is **3** (10001, 10002, 10003). If aggregated without queries, the result is **5** (a total of five rows in two batches). It can be seen that both results are wrong.

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

### Merge-on-write implementation of Unique model

In Merge-on-write implementation, there is no limitation of aggregation model. A new structure delete-bitmap is added to each rowset during loading, to mark some data as overwritten or deleted. With the previous example, after the first batch of data is loaded, the status is as follows:

**batch 1**

| user_id | date       | cost | delete bit |
| ------- | ---------- | ---- | ---------- |
| 10001   | 2017-11-20 | 50   | false      |
| 10002   | 2017-11-21 | 39   | false      |

After the batch2 is loaded, the duplicate rows in the first batch will be marked as deleted, and the status of the two batches of data is as follows

**batch 1**

| user_id | date       | cost | delete bit |
| ------- | ---------- | ---- | ---------- |
| 10001   | 2017-11-20 | 50   | **true**   |
| 10002   | 2017-11-21 | 39   | false      |

**batch 2**

| user\_id | date       | cost | delete bit |
| -------- | ---------- | ---- | ---------- |
| 10001    | 2017-11-20 | 1    | false      |
| 10001    | 2017-11-21 | 5    | false      |
| 10003    | 2017-11-22 | 22   | false      |

When querying, all data marked for deletion in the delete-bitmap will not be read out, so there is no need to do any data aggregation. The number of valid rows in the above data is 4 rows, and the query result should also be 4 rows. 

It is also possible to obtain the result in the least expensive way, that is, the way of "scanning only a certain column of data to obtain the count value" mentioned above.

In the test environment, the performance of the count(*) query in the merge-on-write implementation of the Unique model is more than 10 times faster than that of the aggregation model.

### Duplicate Model

Duplicate model has no limitation of aggregation model. Because the model does not involve aggregate semantics, when doing count (*) query, we can get the correct semantics by choosing a column of queries arbitrarily.

## Key Columns
For the Duplicate,Aggregate and Unique models,The key columns will be given when the table created, but it is actually different: For the Duplicate model, the key columns of the table can be regarded as just "sort columns", not an unique identifier. In aggregate type tables such as Aggregate and Unique models, the key columns are both "sort columns" and "unique identification columns", which were the real "key columns".

## Suggestions for Choosing Data Model

Because the data model was established when the table was built, and **could not be modified. Therefore, it is very important to select an appropriate data model**.

1. Aggregate model can greatly reduce the amount of data scanned and the amount of query computation by pre-aggregation. It is very suitable for report query scenarios with fixed patterns. But this model is not very friendly for count (*) queries. At the same time, because the aggregation method on the Value column is fixed, semantic correctness should be considered in other types of aggregation queries.
2. Uniq model guarantees the uniqueness of primary key for scenarios requiring unique primary key constraints. However, the query advantage brought by pre-aggregation such as ROLLUP cannot be exploited.
   1. For users who have high performance requirements for aggregate queries, it is recommended to use the merge-on-write implementation added since version 1.2.
   2. The Unique model only supports the entire row update. If the user needs unique key with partial update (such as loading multiple source tables into one doris table), you can consider using the Aggregate model, setting the aggregate type of the non-primary key columns to REPLACE_IF_NOT_NULL. For detail, please refer to [CREATE TABLE Manual](../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE.md)
3. Duplicate is suitable for ad-hoc queries of any dimension. Although it is also impossible to take advantage of the pre-aggregation feature, it is not constrained by the aggregation model and can take advantage of the queue-store model (only reading related columns, but not all Key columns).
