---
{
    "title": "Release 1.1.2",
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


In this release, Doris Team has fixed more than 170 issues or performance improvement since 1.1.1. This release is a bugfix release on 1.1 and all users are encouraged to upgrade to this release.

# Features

### New MemTracker

Introduced new MemTracker for both vectorized engine and non-vectorized engine which is more accurate.

### Add API for showing current queries and kill query

### Support read/write emoji of UTF16 via ODBC Table

# Improvements

### Data Lake related improvements

- Improved HDFS ORC File scan performance about 300%. [#11501](https://github.com/apache/doris/pull/11501)

- Support HDFS HA mode when query Iceberg table.

- Support query Hive data created by [Apache Tez](https://tez.apache.org/)

- Add Ali OSS as Hive external support.

### Add support for string and text type in Spark Load


### Add reuse block in non-vectorized engine and have 50% performance improvement in some cases. [#11392](https://github.com/apache/doris/pull/11392)

### Improve like or regex performance

### Disable tcmalloc's aggressive_memory_decommit 

It will have 40% performance gains in load or query.

Currently it is a config, you can change it by set config `tc_enable_aggressive_memory_decommit`.

# Bug Fix

### Some issues about FE that will cause FE failure or data corrupt.

- Add reserved disk config to avoid too many reserved BDB-JE files.**(Serious)**   In an HA environment, BDB JE will retains as many reserved files. The BDB-je log doesn't delete until approaching a disk limit.

- Fix fatal bug in BDB-JE which will cause FE replica could not start correctly or data corrupted.** (Serious)**

### Fe will hang on waitFor_rpc during query and BE will hang in high concurrent scenarios.

[#12459](https://github.com/apache/doris/pull/12459) [#12458](https://github.com/apache/doris/pull/12458) [#12392](https://github.com/apache/doris/pull/12392)

### A fatal issue in vectorized storage engine which will cause wrong result. **(Serious)**

[#11754](https://github.com/apache/doris/pull/11754) [#11694](https://github.com/apache/doris/pull/11694)

### Lots of planner related issues that will cause BE core or in abnormal state.

[#12080](https://github.com/apache/doris/pull/12080) [#12075](https://github.com/apache/doris/pull/12075) [#12040](https://github.com/apache/doris/pull/12040) [#12003](https://github.com/apache/doris/pull/12003) [#12007](https://github.com/apache/doris/pull/12007) [#11971](https://github.com/apache/doris/pull/11971) [#11933](https://github.com/apache/doris/pull/11933) [#11861](https://github.com/apache/doris/pull/11861) [#11859](https://github.com/apache/doris/pull/11859) [#11855](https://github.com/apache/doris/pull/11855) [#11837](https://github.com/apache/doris/pull/11837) [#11834](https://github.com/apache/doris/pull/11834) [#11821](https://github.com/apache/doris/pull/11821) [#11782](https://github.com/apache/doris/pull/11782) [#11723](https://github.com/apache/doris/pull/11723) [#11569](https://github.com/apache/doris/pull/11569)

