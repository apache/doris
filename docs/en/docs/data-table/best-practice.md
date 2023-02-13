---
{
    "title": "Best Practices",
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


# Best Practices

## 1 tabulation

### 1.1 Data Model Selection

Doris data model is currently divided into three categories: AGGREGATE KEY, UNIQUE KEY, DUPLICATE KEY. Data in all three models are sorted by KEY.

1.1.1. AGGREGATE KEY

When AGGREGATE KEY is the same, old and new records are aggregated. The aggregation functions currently supported are SUM, MIN, MAX, REPLACE.

AGGREGATE KEY model can aggregate data in advance and is suitable for reporting and multi-dimensional analysis business.

```
CREATE TABLE site_visit
(
siteid      INT,
City: SMALLINT,
username VARCHAR (32),
pv BIGINT   SUM DEFAULT '0'
)
AGGREGATE KEY(siteid, city, username)
DISTRIBUTED BY HASH(siteid) BUCKETS 10;
```

1.1.2. UNIQUE KEY

When UNIQUE KEY is the same, the new record covers the old record. Before version 1.2, UNIQUE KEY implements the same REPLACE aggregation method as AGGREGATE KEY, and they are essentially the same. We introduced a new merge-on-write implementation for UNIQUE KEY since version 1.2, which have better performance on many scenarios. Suitable for analytical business with updated requirements.

```
CREATE TABLE sales_order
(
orderid     BIGINT,
status      TINYINT,
username VARCHAR (32),
amount      BIGINT DEFAULT '0'
)
KEY (orderid) UNIT
DISTRIBUTED BY HASH(orderid) BUCKETS 10;
```

1.1.3. DUPLICATE KEY

Only sort columns are specified, and the same rows are not merged. It is suitable for the analysis business where data need not be aggregated in advance.

```
CREATE TABLE session_data
(
visitorid SMALLINT,
sessionid   BIGINT,
visit time DATETIME,
City CHAR (20),
province    CHAR(20),
ip. varchar (32),
brower      CHAR(20),
url: VARCHAR (1024)
)
DUPLICATE KEY (visitor time, session time)
DISTRIBUTED BY HASH(sessionid, visitorid) BUCKETS 10;
```

### 1.2 Wide Table vs. Star Schema

When the business side builds tables, in order to adapt to the front-end business, they often do not distinguish between dimension information and indicator information, and define the Schema as a large wide table, this operation is actually not so friendly to the database, we recommend users to use the star model.

* There are many fields in Schema, and there may be more key columns in the aggregation model. The number of columns that need to be sorted in the import process will increase.
* Dimensional information updates are reflected in the whole table, and the frequency of updates directly affects the efficiency of queries.

In the process of using Star Schema, users are advised to use Star Schema to distinguish dimension tables from indicator tables as much as possible. Frequently updated dimension tables can also be placed in MySQL external tables. If there are only a few updates, they can be placed directly in Doris. When storing dimension tables in Doris, more copies of dimension tables can be set up to improve Join's performance.

### 1.3 Partitioning and Bucketing 

Doris supports two-level partitioned storage. The first level is partition, which currently supports both RANGE and LIST partition types, and the second layer is HASH bucket.

1.3.1. Partitioning

Partition is used to divide data into different intervals, which can be logically understood as dividing the original table into multiple sub-tables. Data can be easily managed by partition, for example, to delete data more quickly.

1.3.1.1. Range Partitioning

In business, most users will choose to partition on time, which has the following advantages:

* Differentiable heat and cold data
* Availability of Doris Hierarchical Storage (SSD + SATA)

1.3.1.2. List Partitioning

In business,, users can select cities or other enumeration values for partition.

1.3.2. Hash Bucketing

The data is divided into different buckets according to the hash value.

* It is suggested that columns with large differentiation should be used as buckets to avoid data skew.
* In order to facilitate data recovery, it is suggested that the size of a single bucket should not be too large and should be kept within 10GB. Therefore, the number of buckets should be considered reasonably when building tables or increasing partitions, among which different partitions can specify different buckets.

### 1.4 Sparse Index and Bloom Filter

Doris stores the data in an orderly manner, and builds a sparse index for Doris on the basis of ordered data. The index granularity is block (1024 rows).

Sparse index chooses fixed length prefix in schema as index content, and Doris currently chooses 36 bytes prefix as index.

* When building tables, it is suggested that the common filter fields in queries should be placed in front of Schema. The more distinguishable the query fields are, the more frequent the query fields are.
* One particular feature of this is the varchar type field. The varchar type field can only be used as the last field of the sparse index. The index is truncated at varchar, so if varchar appears in front, the length of the index may be less than 36 bytes. Specifically, you can refer to [data model](./data-model.md), [ROLLUP and query](./hit-the-rollup.md).
* In addition to sparse index, Doris also provides bloomfilter index. Bloomfilter index has obvious filtering effect on columns with high discrimination. If you consider that varchar cannot be placed in a sparse index, you can create a bloomfilter index.

### 1.5 Rollup

Rollup can essentially be understood as a physical index of the original table. When creating Rollup, only some columns in Base Table can be selected as Schema. The order of fields in Schema can also be different from that in Base Table.

Rollup can be considered in the following cases:

1.5.1. Low ratio of data aggregation in the Base Table

This is usually due to the fact that Base Table has more differentiated fields. At this point, you can consider selecting some columns and establishing Rollup.

For the `site_visit'table:

```
site -u visit (siteid, city, username, pv)
```

Siteid may lead to a low degree of data aggregation. If business parties often base their PV needs on city statistics, they can build a city-only, PV-based rollup:

```
ALTER TABLE site_visit ADD ROLLUP rollup_city(city, pv);
```

1.5.2. The prefix index in Base Table cannot be hit

Generally, the way Base Table is constructed cannot cover all query modes. At this point, you can consider adjusting the column order and establishing Rollup.

Database Session

```
session -u data (visitorid, sessionid, visittime, city, province, ip, browser, url)
```

In addition to visitorid analysis, there are Brower and province analysis cases, Rollup can be established separately.

```
ALTER TABLE session_data ADD ROLLUP rollup_brower(brower,province,ip,url) DUPLICATE KEY(brower,province);
```

## Schema Change

Users can modify the Schema of an existing table through the Schema Change operation, currently Doris supports the following modifications:

- Adding and deleting columns
- Modify column types
- Reorder columns
- Adding or modifying Bloom Filter
- Adding or removing bitmap index

For details, please refer to [Schema Change](

## Row Store format
We support row format for olap table to reduce point lookup io cost, but to enable this format you need to spend more disk space for row format store.Currently we store row in an extra column called `row column` for simplicity.Row store is disabled by default, users can enable it by adding the following property when create table
```
"store_row_column" = "true"
```

## Accelerate point query for merge-on-write model
As we provided row store format , we could use such store format to speed up point query performance for merge-on-write model.For point query on primary keys when `enable_unique_key_merge_on_write` enabled, planner will optimize such query and execute in a short path in a light weight RPC interface.Bellow is an example of point query with row store on merge-on-write model:
```
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
UNIQUE KEY(key)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(key) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"enable_unique_key_merge_on_write" = "true",
"light_schema_change" = "true",
"store_row_column" = "true"
);
```
[NOTE]
1. `enable_unique_key_merge_on_write` should be enabled, since we need primary key for quick point lookup in storage engine
2. when condition only contains primary key like `select * from tbl_point_query where key = 123`, such query will go through the short fast path
3. `light_schema_change` should also been enabled since we rely `column unique id` of each columns when doing point query.

### Using `PreparedStatement`
In order to reduce CPU cost for parsing query SQL and SQL expressions, we provide `PreparedStatement` feature in FE fully compatible with mysql protocol (currently only support point queries like above mentioned).Enable it will pre caculate PreparedStatement SQL and expresions and caches it in a session level memory buffer and will be reused later on.We could improve 4x+ performance by using `PreparedStatement` when CPU became hotspot doing such queries.Bellow is an JDBC example of using `PreparedStatement`.

1. Setup JDBC url and enable server side prepared statement
```
url = jdbc:mysql://127.0.0.1:9137/ycsb?useServerPrepStmts=true
``

2. Using `PreparedStatement`
```java
// use `?` for placement holders, readStatement should be reused
PreparedStatement readStatement = conn.prepareStatement("select * from tbl_point_query where key = ?");
...
readStatement.setInt(1234);
ResultSet resultSet = readStatement.executeQuery();
...
readStatement.setInt(1235);
resultSet = readStatement.executeQuery();
...
```



