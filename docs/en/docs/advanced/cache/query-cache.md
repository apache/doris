---
{
    "title": "Query Cache",
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

# Query Cache

## Demand scenario

Most data analysis scenarios are to write less and read more. The data is written once and read multiple times frequently. For example, the dimensions and indicators involved in a report are calculated once in the early morning, but hundreds or even thousands of times a day. Page access, so it is very suitable for caching the result set. In data analysis or BI applications, the following business scenarios exist:

- **High concurrency scenario**, Doris can better support high concurrency, but a single server cannot carry too high QPS
- **Complex Chart Kanban**, complex Dashboard or large-screen application, data comes from multiple tables, and each page has dozens of queries. Although each query only takes tens of milliseconds, the overall query time will be several seconds.
- **Trend Analysis**, for queries within a given date range, indicators are displayed on a daily basis, such as querying the trend of the number of users in the last 7 days. This type of query has a large amount of data, a wide query range, and the query time often takes tens of seconds.
- **User repeated query**, if the product does not have an anti-refresh mechanism, the user repeatedly refreshes the page due to manual error or other reasons, resulting in a large number of repeated SQL submissions.

In the above four scenarios, the solution at the application layer puts the query results into Redis and periodically updates the cache or the user manually refreshes the cache. However, this solution has the following problems:

- **Inconsistent data**, unable to detect data updates, causing users to often see old data
- **Low hit rate**, the entire query result is cached. If the data is written in real time, the cache fails frequently, the hit rate is low and the system load is heavy.
- **Additional Cost**, introducing external cache components will bring system complexity and increase additional costs.

## solution

This partition cache strategy can solve the above problems, giving priority to ensuring data consistency, and on this basis, refining the cache granularity and improving the hit rate, so it has the following characteristics:

- Users do not need to worry about data consistency. Cache invalidation is controlled through versioning. The cached data is consistent with the data queried from BE.
- There are no additional components and costs, the cache results are stored in BE's memory, and users can adjust the cache memory size as needed
- Implemented two caching strategies, SQLCache and PartitionCache, the latter has a finer cache granularity
- Use consistent hashing to solve the problem of BE nodes going online and offline. The caching algorithm in BE is an improved LRU

## scenes to be used

Currently, it supports two methods: SQL Cache and Partition Cache, and supports OlapTable internal table and Hive external table.

SQL Cache: Only SQL statements that are completely consistent will hit the cache. For details, see: sql-cache-manual.md

Partition Cache: Multiple SQLs can hit the cache using the same table partition, so it has a higher hit rate than SQL Cache. For details, see: partition-cache-manual.md

## Monitoring

FE monitoring items:

```text
query_table //The number of tables in Query
query_olap_table //The number of Olap tables in Query
cache_mode_sql //Identify the number of Query whose cache mode is sql
cache_hit_sql //The number of Query hits in Cache with mode sql
query_mode_partition //The number of queries that identify the cache mode as Partition
cache_hit_partition //The number of Query hits through Partition
partition_all //All partitions scanned in Query
partition_hit //Number of partitions hit through Cache

Cache hit rate = (cache_hit_sql + cache_hit_partition) / query_olap_table
Partition hit rate = partition_hit / partition_all
```

BE monitoring items:

```text
query_cache_memory_total_byte //Cache memory size
query_query_cache_sql_total_count //The number of SQL cached
query_cache_partition_total_count //Number of Cache partitions

SQL average data size = cache_memory_total / cache_sql_total
Partition average data size = cache_memory_total / cache_partition_total
```

Other monitoring: You can view the CPU and memory indicators of the BE node, Query Percentile and other indicators in the Query statistics from Grafana, and adjust the Cache parameters to achieve business goals.

## Related parameters

1. cache_result_max_row_count

The maximum number of rows that the query result set can put into the cache. The default is 3000.

```text
vim fe/conf/fe.conf
cache_result_max_row_count=3000
```

2. cache_result_max_data_size

The maximum data size of the query result set placed in the cache is 30M by default. It can be adjusted according to the actual situation, but it is recommended not to set it too large to avoid excessive memory usage. Result sets exceeding this size will not be cached.

```text
vim fe/conf/fe.conf
cache_result_max_data_size=31457280
```

3. cache_last_version_interval_second

The minimum time interval between the latest version of the cached query partition and the current version. Only the query results of partitions that are larger than this interval and have not been updated will be cached. The default is 30, in seconds.

```text
vim fe/conf/fe.conf
cache_last_version_interval_second=30
```

4. query_cache_max_size_mb and query_cache_elasticity_size

query_cache_max_size_mb is the upper memory limit of the cache, query_cache_elasticity_size is the memory size that the cache can stretch. When the total cache size on BE exceeds query_cache_max_size + cache_elasticity_size, it will start to be cleaned up and the memory will be controlled below query_cache_max_size.

These two parameters can be set according to the number of BE nodes, node memory size, and cache hit rate. Calculation method: If 10,000 Queries are cached, each Query caches 1,000 rows, each row is 128 bytes, and is distributed on 10 BEs, then each BE requires about 128M memory (10,000 * 1,000 * 128/10).

```text
vim be/conf/be.conf
query_cache_max_size_mb=256
query_cache_elasticity_size_mb=128
```

5. cache_max_partition_count

Parameters unique to Partition Cache. The maximum number of BE partitions refers to the maximum number of partitions corresponding to each SQL. If it is partitioned by date, it can cache data for more than 2 years. If you want to keep the cache for a longer time, please set this parameter larger and modify the parameters at the same time. cache_result_max_row_count and cache_result_max_data_size.

```text
vim be/conf/be.conf
cache_max_partition_count=1024
```
