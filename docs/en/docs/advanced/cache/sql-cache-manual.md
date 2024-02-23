---
{
    "title": "SQL Cache",
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

# SQL Cache

The SQL statement will hit the cache if it is completely consistent.

## Demand scenarios & solutions

See query-cache.md.

## Design principles

SQLCache stores and obtains the cache based on the SQL signature, the partition ID of the queried table, and the latest version of the partition. The combination of the three determines a cached data set. If any one of them changes, such as the SQL changes, the query fields or conditions are different, or the version changes after the data is updated, the cache will not be hit.

If multiple tables are joined, the most recently updated partition ID and latest version number are used. If one of the tables is updated, the partition ID or version number will be different, and the cache will not be hit.

SQLCache is more suitable for T+1 update scenarios. Data is updated in the early morning. The first query obtains the results from BE and puts them into the cache. Subsequent queries of the same nature obtain the results from the cache. Real-time update data can also be used, but there may be a problem of low hit rate.

Currently supports OlapTable internal table and Hive external table.

## Usage

Make sure cache_enable_sql_mode=true in fe.conf (default is true)

```text
vim fe/conf/fe.conf
cache_enable_sql_mode=true
```

Set variables in MySQL command line

```sql
MySQL [(none)]> set [global] enable_sql_cache=true;
```

Note: global is a global variable and does not refer to the current session variable.

## Cache conditions

After the first query, if the following three conditions are met, the query results will be cached.

1. (Current time - the last update time of the queried partition) is greater than cache_last_version_interval_second in fe.conf.

2. The number of query result rows is less than cache_result_max_row_count in fe.conf.

3. The query result bytes is less than cache_result_max_data_size in fe.conf.

For detailed parameter introduction and unfinished matters, see query-cache.md.

## Unfinished business

- SQL contains functions that generate random values, such as random(). Using QueryCache will cause the query results to lose their randomness, and the same results will be obtained every time they are executed.

- Similar SQL, 2 indicators were queried before, and now 3 indicators are queried. Can the cache of 2 indicators be used? Not currently supported
