---
{
    "title": "Partition Cache",
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

# Partition Cache

Cache hits can occur when multiple SQLs use the same table partition.

```
**Partition Cache is an experimental feature and is not well maintained. Use it with caution**
```

## Demand scenarios & solutions

See query-cache.md.

## Design principles

1. SQL can be split in parallel, Q = Q1 ∪ Q2 ... ∪ Qn, R= R1 ∪ R2 ... ∪ Rn, Q is the query statement, R is the result set

2. SQL only uses DATE, INT, and BIGINT types of partition field aggregation, and only scans one partition. Therefore, it does not support partitioning by day, but only supports partitioning by year and month.

3. Cache the results of some dates in the query result set, and then reduce the date range scanned in SQL. In essence, PartitionCache does not reduce the number of partitions scanned, but also reduces the date range scanned, thereby reducing the amount of scanned data.

In addition some restrictions:

- Only supports grouping by partition fields, not by other fields. Grouping by other fields may cause the group data to be updated, which will cause the cache to become invalid.

- Only the first half, the second half and all hits of the result set are supported in the cache. The result set is not supported to be divided into several parts by cached data, and the dates of the result set must be continuous. If there is no data in the result set on a certain day, then only this Dates one day older will be cached.

- If the predicate has columns outside the partition, you must add brackets to the partition predicate `where k1 = 1 and (key >= "2023-10-18" and key <= "2021-12-01")`

- The number of days in the query must be greater than 1 and less than cache_result_max_row_count, otherwise the partition cache cannot be used.

- The predicate of the partition field can only be key >= a and key <= b or key = a or key = b or key in (a,b,c).

## Usage

Make sure cache_enable_partition_mode=true in fe.conf (default is true)

```text
vim fe/conf/fe.conf
cache_enable_partition_mode=true
```

Set variables in MySQL command line

```sql
MySQL [(none)]> set [global] enable_partition_cache=true;
```

If two caching strategies are enabled at the same time, you need to pay attention to the following parameters:

```text
cache_last_version_interval_second=30
```

If the interval between the latest version of the partition and the present is greater than cache_last_version_interval_second, the entire query result will be cached first. If it is less than this interval, if it meets the conditions of PartitionCache, the PartitionCache data will be pressed.

For detailed parameter introduction and unfinished matters, see query-cache.md.

## Unfinished business

Split into read-only partitions and updateable partitions, read-only partitions are cached, update partitions are not cached

As above, query the number of daily users in the last 7 days. For example, if partitioned by date, the data will only be written to the partition of the current day. The data of other partitions other than the current day are fixed. Under the same query SQL, query a certain area that is not updated. The partition indicators are all fixed. As follows, the number of users in the previous 7 days is queried on 2020-03-09. The data from 2020-03-03 to 2020-03-07 comes from the cache. The first query on 2020-03-08 comes from the partition, and subsequent queries come from the cache. , 2020-03-09 because it was written continuously that day, so it comes from the partition.

Therefore, to query N days of data, the data is updated for the most recent D days. Similar queries with different date ranges every day only need to query D partitions. The other parts come from the cache, which can effectively reduce the cluster load and query time.

Implementation principle example:

```sql
MySQL [(none)]> SELECT eventdate,count(userid) FROM testdb.appevent WHERE eventdate>="2020-03-03" AND eventdate<="2020-03-09" GROUP BY eventdate ORDER BY eventdate;
+----------------+-----------------+
| eventdate | count(`userid`) |
+----------------+-----------------+
| 2020-03-03 | 15 |
| 2020-03-04 | 20 |
| 2020-03-05 | 25 |
| 2020-03-06 | 30 |
| 2020-03-07 | 35 |
| 2020-03-08 | 40 | //The first time comes from the partition, and the subsequent ones come from the cache
| 2020-03-09 | 25 | //From partition
+----------------+-----------------+
7 rows in set (0.02 sec)
```

In PartitionCache, the cached first-level Key is the 128-bit MD5 signature of the SQL after removing the partition conditions. The following is the rewritten SQL to be signed:

```sql
SELECT eventdate,count(userid) FROM testdb.appevent GROUP BY eventdate ORDER BY eventdate;
```

The cached second-level key is the content of the partition field of the query result set, such as the content of the eventdate column of the query result above. The ancillary information of the second-level key is the version number and version update time of the partition.

The following demonstrates the process of executing the above SQL for the first time on 2020-03-09:

1. Get data from cache

```text
+----------------+-----------------+
| 2020-03-03 | 15 |
| 2020-03-04 | 20 |
| 2020-03-05 | 25 |
| 2020-03-06 | 30 |
| 2020-03-07 | 35 |
+----------------+-----------------+
```

2. SQL and data to get data from BE

```sql
SELECT eventdate,count(userid) FROM testdb.appevent WHERE eventdate>="2020-03-08" AND eventdate<="2020-03-09" GROUP BY eventdate ORDER BY eventdate;

+----------------+-----------------+
| 2020-03-08 | 40 |
+----------------+-----------------+
| 2020-03-09 | 25 |
+----------------+-----------------+
```

3. The last data sent to the terminal

```text
+----------------+-----------------+
| eventdate | count(`userid`) |
+----------------+-----------------+
| 2020-03-03 | 15 |
| 2020-03-04 | 20 |
| 2020-03-05 | 25 |
| 2020-03-06 | 30 |
| 2020-03-07 | 35 |
| 2020-03-08 | 40 |
| 2020-03-09 | 25 |
+----------------+-----------------+
```

4. Data sent to cache

```text
+----------------+-----------------+
| 2020-03-08 | 40 |
+----------------+-----------------+
```

Partition cache is suitable for partitioning by date, some partitions are updated in real time, and the query SQL is relatively fixed.

The partition field can also be other fields, but it needs to be ensured that only a small number of partitions are updated.
