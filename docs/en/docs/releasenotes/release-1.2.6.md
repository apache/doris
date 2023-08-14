---
{
    "title": "Release 1.2.6",
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


# Behavior Change

- Add a BE configuration item `allow_invalid_decimalv2_literal` to control whether can import data that exceeding the decimal's precision, for compatibility with previous logic.

# Query

- Fix several query planning issues.
- Support `sql_select_limit` session variable.
- Optimize query cold run performance.
- Fix expr context memory leak.
- Fix the issue that the `explode_split` function was executed incorrectly in some cases.

## Multi Catalog

- Fix the issue that synchronizing hive metadata caused FE replay edit log to fail.
- Fix `refresh catalog` operation causing FE OOM.
- Fix the issue that jdbc catalog cannot handle `0000-00-00` correctly.
- Fixed the issue that the kerberos ticket cannot be refreshed automatically.
- Optimize the partition pruning performance of hive.
- Fix the inconsistent behavior of trino and presto in jdbc catalog.
- Fix the issue that hdfs short-circuit read could not be used to improve query efficiency in some environments.
- Fix the issue that the iceberg table on CHDFS could not be read.

# Storage

- Fix the wrong calculation of delete bitmap in MOW table.
- Fix several BE memory issues.
- Fix snappy compression issue.
- Fix the issue that jemalloc may cause BE to crash in some cases.

# Others

- Fix several java udf related issues.
- Fix the issue that the `recover table` operation incorrectly triggered the creation of dynamic partitions.
- Fix timezone when importing orc files via broker load.
- Fix the issue that the newly added `PERCENT` keyword caused the replay metadata of the routine load job to fail.
- Fix the issue that the `truncate` operation failed to acts on a non-partitioned table.
- Fix the issue that the mysql connection was lost due to the `show snapshot` operation.
- Optimize the lock logic to reduce the probability of lock timeout errors when creating tables.
- Add session variable `have_query_cache` to be compatible with some old mysql clients.
- Optimize the error message when encountering an error of loading.

# Big Thanks

Thanks all who contribute to this release:

@amorynan

@BiteTheDDDDt

@caoliang-web

@dataroaring

@Doris-Extras

@dutyu

@Gabriel39

@HHoflittlefish777

@htyoung

@jacktengg

@jeffreys-cat

@kaijchen

@kaka11chen

@Kikyou1997

@KnightLiJunLong

@liaoxin01

@LiBinfeng-01

@morningman

@mrhhsg

@sohardforaname

@starocean999

@vinlee19

@wangbo

@wsjz

@xiaokang

@xinyiZzz

@yiguolei

@yujun777

@Yulei-Yang

@zhangstar333

@zy-kkk

