---
{
    "title": "Release 1.2.5",
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

In version 1.2.5, the Doris team has fixed nearly 210 issues or performance improvements since the release of version 1.2.4. At the same time, version 1.2.5 is also an iterative version of version 1.2.4, which has higher stability. It is recommended that all users upgrade to this version.

# Behavior Changed

- The `start_be.sh` script will check that the maximum number of file handles in the system must be greater than or equal to 65536, otherwise the startup will fail.

- The BE configuration item `enable_quick_compaction` is set to true by default. The Quick Compaction is enabled by default. This feature is used to optimize the problem of small files in the case of large batch import.

- After modifying the dynamic partition attribute of the table, it will no longer take effect immediately, but wait for the next task scheduling of the dynamic partition table to avoid some deadlock problems.

# Improvement

- Optimize the use of bthread and pthread to reduce the RPC blocking problem during the query process.

- A button to download Profile is added to the Profile page of the FE web UI.

- Added FE configuration `recover_with_skip_missing_version`, which is used to query to skip the problematic replica under certain failure conditions.

- The row-level permission function supports external Catalog.

- Hive Catalog supports automatic refreshing of kerberos tickets on the BE side without manual refreshing.

- JDBC Catalog supports tables under the MySQL/ClickHouse system database (`information_schema`).

# Bug Fixes

- Fixed the problem of incorrect query results caused by low-cardinality column optimization

- Fixed several authentication and compatibility issues accessing HDFS.

- Fixed several issues with float/double and decimal types.

- Fixed several issues with date/datetimev2 types.

- Fixed several query execution and planning issues.

- Fixed several issues with JDBC Catalog.

- Fixed several query-related issues with Hive Catalog, and Hive Metastore metadata synchronization issues.

- Fix the problem that the result of `SHOW LOAD PROFILE` statement is incorrect.

- Fixed several memory related issues.

- Fixed several issues with `CREATE TABLE AS SELECT` functionality.

- Fix the problem that the jsonb type causes BE to crash on CPU that do not support avx2.

- Fixed several issues with dynamic partitions.

- Fixed several issues with TOPN query optimization.

- Fixed several issues with the Unique Key Merge-on-Write table model.

# Big Thanks

58 contributors participated in the improvement and release of 1.2.5, and thank them for their hard work and dedication:

@adonis0147

@airborne12

@AshinGau

@BePPPower

@BiteTheDDDDt

@caiconghui

@CalvinKirs

@cambyzju

@caoliang-web

@dataroaring

@Doris-Extras

@dujl

@dutyu

@fsilent

@Gabriel39

@gitccl

@gnehil

@GoGoWen

@gongzexin

@HappenLee

@herry2038

@jacktengg

@Jibing-Li

@kaka11chen

@Kikyou1997

@LemonLiTree

@liaoxin01

@LiBinfeng-01

@luwei16

@Moonm3n

@morningman

@mrhhsg

@Mryange

@nextdreamblue

@nsnhuang

@qidaye

@Shoothzj

@sohardforaname

@stalary

@starocean999

@SWJTU-ZhangLei

@wsjz

@xiaokang

@xinyiZzz

@yangzhg

@yiguolei

@yixiutt

@yujun777

@Yulei-Yang

@yuxuan-luo

@zclllyybb

@zddr

@zenoyang

@zhangstar333

@zhannngchen

@zxealous

@zy-kkk

@zzzzzzzs
