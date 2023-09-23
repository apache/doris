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

## Demand Scenarios

In most data analysis scenarios, write less and read more. Data is written once and read frequently. For example, the dimensions and indicators involved in a report are calculated at one time in the early morning, but there are hundreds or even thousands of times every day. page access, so it is very suitable for caching the result set. In data analysis or BI applications, the following business scenarios exist:

- **High concurrency scenario**, Doris can better support high concurrency, but a single server cannot carry too high QPS
- **Kanban for complex charts**, complex Dashboard or large-screen applications, the data comes from multiple tables, each page has dozens of queries, although each query is only tens of milliseconds, but the overall query time will be in a few seconds
- **Trend analysis**, the query for a given date range, the indicators are displayed by day, such as querying the trend of the number of users in the last 7 days, this type of query has a large amount of data and a wide range of queries, and the query time often takes tens of seconds
- **User repeated query**, if the product does not have an anti-reload mechanism, the user repeatedly refreshes the page due to hand error or other reasons, resulting in a large number of repeated SQL submissions

In the above four scenarios, the solution at the application layer is to put the query results in Redis, update the cache periodically or manually refresh the cache by the user, but this solution has the following problems:

- **Data inconsistency**, unable to perceive the update of data, causing users to often see old data
- **Low hit rate**, cache the entire query result, if the data is written in real time, the cache is frequently invalidated, the hit rate is low and the system load is heavy
- **Additional cost**, the introduction of external cache components will bring system complexity and increase additional costs

## Solution

This partitioned caching strategy can solve the above problems, giving priority to ensuring data consistency. On this basis, the cache granularity is refined and the hit rate is improved. Therefore, it has the following characteristics:

- Users do not need to worry about data consistency, cache invalidation is controlled by version, and the cached data is consistent with the data queried from BE
- No additional components and costs, cached results are stored in BE's memory, users can adjust the cache memory size as needed
- Implemented two caching strategies, SQLCache and PartitionCache, the latter has a finer cache granularity
- Use consistent hashing to solve the problem of BE nodes going online and offline. The caching algorithm in BE is an improved LRU

## SQL Cache

SQLCache stores and retrieves the cache according to the SQL signature, the partition ID of the queried table, and the latest version of the partition. The combination of the three determines a cached data set. If any one changes, such as SQL changes, such as query fields or conditions are different, or the version changes after the data is updated, the cache will not be hit.

If multiple tables are joined, use the latest updated partition ID and the latest version number. If one of the tables is updated, the partition ID or version number will be different, and the cache will also not be hit.

SQLCache is more suitable for T+1 update scenarios. Data is updated in the early morning. The results obtained from the BE for the first query are put into the cache, and subsequent identical queries are obtained from the cache. Real-time update data can also be used, but there may be a problem of low hit rate. You can refer to the following PartitionCache.

## Partition Cache

### Design Principles

1. SQL can be split in parallel, Q = Q1 ∪ Q2 ... ∪ Qn, R= R1 ∪ R2 ... ∪ Rn, Q is the query statement, R is the result set
2. Split into read-only partitions and updatable partitions, read-only partitions are cached, and update partitions are not cached

As above, query the number of users per day in the last 7 days, such as partitioning by date, the data is only written to the partition of the day, and the data of other partitions other than the day is fixed. Under the same query SQL, query a certain part that does not update Partition indicators are fixed. As follows, the number of users in the first 7 days is queried on 2020-03-09, the data from 2020-03-03 to 2020-03-07 comes from the cache, the first query on 2020-03-08 comes from the partition, and subsequent queries come from the cache , 2020-03-09 is from the partition because it is constantly being written that day.

Therefore, when querying N days of data, the data is updated on the most recent D days. Every day is only a query with a different date range and a similar query. Only D partitions need to be queried, and the other parts are from the cache, which can effectively reduce the cluster load and reduce query time.

```sql
MySQL [(none)]> SELECT eventdate,count(userid) FROM testdb.appevent WHERE eventdate>="2020-03-03" AND eventdate<="2020-03-09" GROUP BY eventdate ORDER BY eventdate;
+------------+-----------------+
| eventdate  | count(`userid`) |
+------------+-----------------+
| 2020-03-03 |              15 |
| 2020-03-04 |              20 |
| 2020-03-05 |              25 |
| 2020-03-06 |              30 |
| 2020-03-07 |              35 |
| 2020-03-08 |              40 | //First from partition, subsequent from cache
| 2020-03-09 |              25 | //from partition
+------------+-----------------+
7 rows in set (0.02 sec)
```

In PartitionCache, the first-level key of the cache is the 128-bit MD5 signature of the SQL after the partition condition is removed. The following is the rewritten SQL to be signed:

```sql
SELECT eventdate,count(userid) FROM testdb.appevent GROUP BY eventdate ORDER BY eventdate;
```

The cached second-level key is the content of the partition field of the query result set, such as the content of the eventdate column of the query result above, and the auxiliary information of the second-level key is the version number and version update time of the partition.

The following demonstrates the process of executing the above SQL for the first time on 2020-03-09:

1. Get data from cache

```text
+------------+-----------------+
| 2020-03-03 |              15 |
| 2020-03-04 |              20 |
| 2020-03-05 |              25 |
| 2020-03-06 |              30 |
| 2020-03-07 |              35 |
+------------+-----------------+
```

1. SQL and data to get data from BE SQL and data to get data from BE

```sql
SELECT eventdate,count(userid) FROM testdb.appevent WHERE eventdate>="2020-03-08" AND eventdate<="2020-03-09" GROUP BY eventdate ORDER BY eventdate;

+------------+-----------------+
| 2020-03-08 |              40 |
+------------+-----------------+
| 2020-03-09 |              25 | 
+------------+-----------------+
```

1. The last data sent to the terminal

```text
+------------+-----------------+
| eventdate  | count(`userid`) |
+------------+-----------------+
| 2020-03-03 |              15 |
| 2020-03-04 |              20 |
| 2020-03-05 |              25 |
| 2020-03-06 |              30 |
| 2020-03-07 |              35 |
| 2020-03-08 |              40 |
| 2020-03-09 |              25 |
+------------+-----------------+
```

1. data sent to cache

```text
+------------+-----------------+
| 2020-03-08 |              40 |
+------------+-----------------+
```

Partition cache is suitable for partitioning by date, some partitions are updated in real time, and the query SQL is relatively fixed.

Partition fields can also be other fields, but need to ensure that only a small number of partition updates.

### Some Restrictions

- Only OlapTable is supported, other tables such as MySQL have no version information and cannot sense whether the data is updated
- Only supports grouping by partition field, does not support grouping by other fields, grouping by other fields, the grouped data may be updated, which will cause the cache to be invalid
- Only the first half of the result set, the second half of the result set and all cache hits are supported, and the result set is not supported to be divided into several parts by the cached data

## How to Use

> NOTE:
>
>   In the following scenarios, the cache result is wrong
>   1. Use session variable: default_order_by_limit, sql_select_limit
>   2. Use var = cur_date(), var = random() functions that generate random values
>
>   There may be other cases where the cache result is wrong, so it is recommended to enable it only in controllable scenarios such as reports.

### Enable SQLCache

Make sure cache_enable_sql_mode=true in fe.conf (default is true)

```text
vim fe/conf/fe.conf
cache_enable_sql_mode=true
```

Setting variables in MySQL command line

```sql
MySQL [(none)]> set [global] enable_sql_cache=true;
```

Note: global is a global variable, not referring to the current session variable

### Enable Partition Cache

Make sure cache_enable_partition_mode=true in fe.conf (default is true)

```text
vim fe/conf/fe.conf
cache_enable_partition_mode=true
```

Setting variables in MySQL command line

```sql
MySQL [(none)]> set [global] enable_partition_cache=true;
```

If two caching strategies are enabled at the same time, the following parameters need to be paid attention to:

```text
cache_last_version_interval_second=900
```

If the interval between the latest version of the partition is greater than cache_last_version_interval_second, the entire query result will be cached first. If it is less than this interval, if it meets the conditions of PartitionCache, press PartitionCache data.

### Monitoring

FE monitoring items:

```text
query_table          //Number of tables in Query
query_olap_table     //Number of Olap tables in Query
cache_mode_sql       //Identify the number of queries whose cache mode is sql
cache_hit_sql        //The number of Cache hits by Query with mode sql
query_mode_partition //Identify the number of queries whose cache mode is Partition
cache_hit_partition  //Number of queries hit by Partition
partition_all        //All partitions scanned in Query
partition_hit        //Number of partitions hit by Cache

Cache hit ratio = (cache_hit_sql + cache_hit_partition) / query_olap_table
Partition hit ratio = partition_hit / partition_all
```

BE's monitoring items:

```text
query_cache_memory_total_byte     //Cache memory size
query_query_cache_sql_total_count //Number of SQL in Cache
query_cache_partition_total_count //Number of Cache partitions

SQL average data size = cache_memory_total / cache_sql_total
Partition average data size = cache_memory_total / cache_partition_total
```

Other monitoring: You can view the CPU and memory indicators of the BE node, the Query Percentile and other indicators in the Query statistics from Grafana, and adjust the Cache parameters to achieve business goals.

### Optimization Parameters

The configuration item cache_result_max_row_count of FE, the maximum number of rows in the cache for the query result set, FE configuration item cache_result_max_data_size, the maximum data size of the query result set put into the cache, can be adjusted according to the actual situation, but it is recommended not to set it too large to avoid taking up too much memory, and the result set exceeding this size will not be cached.

```text
vim fe/conf/fe.conf
cache_result_max_row_count=3000
```

The maximum number of partitions in BE cache_max_partition_count refers to the maximum number of partitions corresponding to each SQL. If it is partitioned by date, it can cache data for more than 2 years. If you want to keep the cache for a longer time, please set this parameter to a larger value and modify it at the same time. Parameter of cache_result_max_row_count and cache_result_max_data_size.

```text
vim be/conf/be.conf
cache_max_partition_count=1024
```

The cache memory setting in BE consists of two parameters, query_cache_max_size and query_cache_elasticity_size (in MB). If the memory exceeds query_cache_max_size + cache_elasticity_size, it will start to clean up and control the memory to below query_cache_max_size. These two parameters can be set according to the number of BE nodes, node memory size, and cache hit rate.

```text
query_cache_max_size_mb=256
query_cache_elasticity_size_mb=128
```

Calculation method:

If 10000 queries are cached, each query caches 1000 rows, each row is 128 bytes, distributed on 10 BEs, then each BE requires about 128M memory (10000 * 1000 * 128/10).

## Unfinished Matters

- Can the data of T+1 also be cached by Partition? Currently not supported
- Similar SQL, 2 indicators were queried before, but now 3 indicators are queried. Can the cache of 2 indicators be used? Not currently supported
- Partition by date, but need to aggregate data by week dimension, is PartitionCache available? Not currently supported
