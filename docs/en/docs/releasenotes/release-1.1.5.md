---
{
    "title": "Release 1.1.5",
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

In this release, Doris Team has fixed about 36 issues or performance improvement since 1.1.4. This release is a bugfix release on 1.1 and all users are encouraged to upgrade to this release.

# Behavior Changes

When alias name is same as the original column name like "select year(birthday) as birthday" and use it in group by, order by , having clause, doris's behavior is different from MySQL in the past. In this release, we make it follow MySQL's behavior. Group by and having clause will use original column at first and order by will use alias first. It maybe a litter confuse here so there is a simple advice here, you'd better not use an alias the same as original column name.

# Features

Add support of murmur_hash3_64. [#14636](https://github.com/apache/doris/pull/14636)

# Improvements

Add timezone cache for convert_tz to improve performance. [#14616](https://github.com/apache/doris/pull/14616)

Sort result by tablename when call show clause. [#14492](https://github.com/apache/doris/pull/14492)

# Bug Fix

Fix coredump when there is a if constant expr in select clause.  [#14858](https://github.com/apache/doris/pull/14858)

ColumnVector::insert_date_column may crashed. [#14839](https://github.com/apache/doris/pull/14839)

Update high_priority_flush_thread_num_per_store default value to 6 and it will improve the load performance. [#14775](https://github.com/apache/doris/pull/14775)

Fix quick compaction core.  [#14731](https://github.com/apache/doris/pull/14731)

Partition column is not duplicate key, spark load will throw IndexOutOfBounds error. [#14661](https://github.com/apache/doris/pull/14661)

Fix a memory leak problem in VCollectorIterator. [#14549](https://github.com/apache/doris/pull/14549)

Fix create table like when having sequence column. [#14511](https://github.com/apache/doris/pull/14511)

Using avg rowset to calculate batch size instead of using total_bytes since it costs a lot of cpu. [#14273](https://github.com/apache/doris/pull/14273)

Fix right outer join core with conjunct. [#14821](https://github.com/apache/doris/pull/14821)

Optimize policy of tcmalloc gc.  [#14777](https://github.com/apache/doris/pull/14777) [#14738](https://github.com/apache/doris/pull/14738) [#14374](https://github.com/apache/doris/pull/14374)


