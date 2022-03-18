---
{
    "title": "Best Practices",
    "language": "en"
}
---

# Best Practices

## 1 tabulation

### 1.1 Data Model Selection

Doris data model is currently divided into three categories: AGGREGATE KEY, UNIQUE KEY, DUPLICATE KEY. Data in all three models are sorted by KEY.

1. AGGREGATE KEY

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

2. KEY UNIQUE

When UNIQUE KEY is the same, the new record covers the old record. At present, UNIQUE KEY implements the same RPLACE aggregation method as GGREGATE KEY, and they are essentially the same. Suitable for analytical business with updated requirements.

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

3. DUPLICATE KEY

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

In order to adapt to the front-end business, business side often does not distinguish dimension information from indicator information, but defines Schema as a wide table. For Doris, the performance of such wide gauges is often unsatisfactory:

* There are many fields in Schema, and there may be more key columns in the aggregation model. The number of columns that need to be sorted in the import process will increase.
* Dimensional information updates are reflected in the whole table, and the frequency of updates directly affects the efficiency of queries.

In the process of using Star Schema, users are advised to use Star Schema to distinguish dimension tables from indicator tables as much as possible. Frequently updated dimension tables can also be placed in MySQL external tables. If there are only a few updates, they can be placed directly in Doris. When storing dimension tables in Doris, more copies of dimension tables can be set up to improve Join's performance.

### 1.3 Partitions and Barrels

Doris supports two-level partitioned storage. The first layer is RANGE partition and the second layer is HASH bucket.

1. RANGE分区(partition)

The RANGE partition is used to divide data into different intervals, which can be logically understood as dividing the original table into multiple sub-tables. In business, most users will choose to partition on time, which has the following advantages:

* Differentiable heat and cold data
* Availability of Doris Hierarchical Storage (SSD + SATA)
* Delete data by partition more quickly

2. HASH分桶(bucket)

The data is divided into different buckets according to the hash value.

* It is suggested that columns with large differentiation should be used as buckets to avoid data skew.
* In order to facilitate data recovery, it is suggested that the size of a single bucket should not be too large and should be kept within 10GB. Therefore, the number of buckets should be considered reasonably when building tables or increasing partitions, among which different partitions can specify different buckets.

### 1.4 Sparse Index and Bloom Filter

Doris stores the data in an orderly manner, and builds a sparse index for Doris on the basis of ordered data. The index granularity is block (1024 rows).

Sparse index chooses fixed length prefix in schema as index content, and Doris currently chooses 36 bytes prefix as index.

* When building tables, it is suggested that the common filter fields in queries should be placed in front of Schema. The more distinguishable the query fields are, the more frequent the query fields are.
* One particular feature of this is the varchar type field. The varchar type field can only be used as the last field of the sparse index. The index is truncated at varchar, so if varchar appears in front, the length of the index may be less than 36 bytes. Specifically, you can refer to [data model, ROLLUP and prefix index] (. / data-model-rollup. md).
* In addition to sparse index, Doris also provides bloomfilter index. Bloomfilter index has obvious filtering effect on columns with high discrimination. If you consider that varchar cannot be placed in a sparse index, you can create a bloomfilter index.

### 1.5 Physical and Chemical View (rollup)

Rollup can essentially be understood as a physical index of the original table. When creating Rollup, only some columns in Base Table can be selected as Schema. The order of fields in Schema can also be different from that in Base Table.

Rollup can be considered in the following cases:

1. Base Table 中数据聚合度不高。

This is usually due to the fact that Base Table has more differentiated fields. At this point, you can consider selecting some columns and establishing Rollup.

For the `site_visit'table:

```
site -u visit (siteid, city, username, pv)
```

Siteid may lead to a low degree of data aggregation. If business parties often base their PV needs on city statistics, they can build a city-only, PV-based ollup:

```
ALTER TABLE site_visit ADD ROLLUP rollup_city(city, pv);
```

2. The prefix index in Base Table cannot be hit

Generally, the way Base Table is constructed cannot cover all query modes. At this point, you can consider adjusting the column order and establishing Rollup.

Database Session

```
session -u data (visitorid, sessionid, visittime, city, province, ip, browser, url)
```

In addition to visitorid analysis, there are Brower and province analysis cases, Rollup can be established separately.

```
ALTER TABLE session_data ADD ROLLUP rollup_brower(brower,province,ip,url) DUPLICATE KEY(brower,province);
```

## 2 Schema Change

Doris中目前进行 Schema Change 的方式有三种：Sorted Schema Change，Direct Schema Change, Linked Schema Change。

1. Sorted Schema Change

The sorting of columns has been changed and the data needs to be reordered. For example, delete a column in a sorted column and reorder the fields.

```
ALTER TABLE site_visit DROP COLUMN city;
```

2. Direct Schema Change: There is no need to reorder, but there is a need to convert the data. For example, modify the type of column, add a column to the sparse index, etc.

```
ALTER TABLE site_visit MODIFY COLUMN username varchar(64);
```

3. Linked Schema Change: 无需转换数据，直接完成。例如加列操作。

```
ALTER TABLE site_visit ADD COLUMN click bigint SUM default '0';
```

Schema is recommended to be considered when creating tables so that Schema can be changed more quickly.
