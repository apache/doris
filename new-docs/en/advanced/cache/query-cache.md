---
{
    "title": "QUERY CACHE",
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

# QUERY CACHE

## 1 Demond

Although the corresponding cache is also made in the database storage layer, the cache in the database storage layer is generally aimed at the query content, and the granularity is too small. Generally, only when the data in the table is not changed can the corresponding cache of the database play a role. However, this can not reduce the huge IO pressure brought by the addition, deletion and query of the database by the business system. Therefore, the database cache technology was born here to realize the cache of hot data, improve the response speed of the application, and greatly relieve the pressure of the back-end database

- High concurrency scenarios
  Doris have a well support for high concurrency while single sever is unable to load too high QPS.

- Complex Graph Dashboard
  It is not uncommon to see that,  data of the complex Dashboard and the large screen applications come from many table together which have tens of queries in a single page.Even though every single query cost only few milliseconds, the total queries would cost seconds.

- Trend Analysis
  In some scenarios, the queries are in a given date range , the index is shown by date.For example, we want to query the trend of the number of user in the last 7 days.This type of queries has a large amount of data and a wide range of fields, and the queries often takes tens of seconds.

- User repeated query
  If the product does not have an anti-re-flash mechanism, the user accidentally  refreshes the page repeatedly due many reasons, which resulting in submitting a large number of repeated SQL

In the above four scenarios, we have solutions at the application layer. We put the result of queries in the Redis and  update the cache periodically or the user update the cache manually.However, this solution has the following problems:

- Inconsistence of data , we are unable to sense the update of data, causing users to often see old data

- Low hit rate, we usually cache the whole result of query.If the data is writed real-time, we would often failed in cache, resulting in low hit rate and overload for the system.

- Extra Cost we introduce external cache components, which will bring system complexity and increase additional costs.

## 2 Solutions

At present, we design two modules: result cache and partition cache

## 3 Explanation of terms

1. result cache

SQL directly caches the result collection of queries for users

2. partition cache

In the partition granularity, cache the results of each partition query

## 4 Design

### 1 `result cache`

result_cache is divided into two types. The first type is result_ cache_ The second type of TTL is result_ cache_ version

#### `result_cache_ttl`

result_ cache_ ttl  variable is set in the user session. The user can customize whether to turn it on or not. The TTL time is used to determine whether the user's SQL uses cache. The correctness of the data is not guaranteed when the data is changed`

The cache is stored and retrieved according to the user connected and the query SQL. If it exceeds the cache expiration time, the cache will not be hit and the cache will be cleaned

#### ` result_cache_version`

result_ cache_ version stores and fetches the cache according to the signature of SQL, partition ID of the query table, latest version of partition. The combination of the three determines a cache dataset. If any one of them changes, such as SQL changes, query fields or conditions are not the same, or the version after data update changes, the cache will not be hit.

If multiple tables are joined, the latest partition ID and the latest version number are used. If one of the tables is updated, the partition ID or version number will be different, and the cache will not be hit.

### 2 `partition_cache`

1. SQL can be split in parallel, Q = Q1 ∪ Q2 ... ∪ Qn, R= R1 ∪ R2 ... ∪ Rn, Q is the query statement and R is the result set
2. Split into read-only partition and updatable partition, read-only partition cache, update partition not cache

## 5 usage

|cache type|usage|
|--|--|
|result_cache_ttl|Mainly solve the scenario of high QPS and repeated query by users|
|result_cache_version|It mainly solves the scenario that the whole table has not changed for a long time|
|partition_cache|It mainly solves the scenario that the historical partition does not change|

## 6 parameter

###  fe

1. `cache_per_query_max_row_count`
- Cache the maximum number of rows per query
- The default value is 3000

2. `cache_per_query_max_size_In_bytes`
- The size of each query in bytes
- The default value is 1MB

3. `result_cache_ttl_In_milliseconds`
- Cache duration of result cache
- The default value is 3S

### be

1. `cache_max_partition_count`
- Be maximum number of partitions cache_ max_ partition_ Count refers to the maximum number of partitions corresponding to each SQL. If the partition is based on date, the data can be cached for more than 2 years. If you want to keep the cache for a longer time, please set this parameter larger and modify the cache_ result_ max_ row_ Count parameter.
- Default value : 1024

2. `cache_max_size_in_mb` `cache_elasticity_size_in_mb` 
- The cache memory setting in backend has two parameters: cache_max_size_In_mb(256) and cache_elasticity_size_In_mb(128), memory exceeds cache_ max_ size_In_mb+cache_elasticity_size_In_mb will clean up and control the memory to cache_max_size_In_mb. These two parameters can be set according to the number of be nodes, the memory size of nodes, and cache hit rate.

## 7 how to use

- use enable_result_cache_ttl
```
set `global`  enable_result_cache_ttl =true
```

- use enable_result_cache_version
```
set `global` enable_result_cache_version = true
```

- use enable_partition_cache
```
set `global` enable_partition_cache = true
```