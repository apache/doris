---
{
    "title": "Release 1.2.2",
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

# New Features

### Lakehouse 

- Support automatic synchronization of Hive metastore.

- Support reading the Iceberg Snapshot, and viewing the Snapshot history.

- JDBC Catalog supports PostgreSQL, Clickhouse, Oracle, SQLServer

- JDBC Catalog supports Insert operation

Reference: [https://doris.apache.org/docs/dev/lakehouse/multi-catalog/](https://doris.apache.org/docs/dev/lakehouse/multi-catalog/)

### Auto Bucket

 Set and scale the number of buckets for different partitions to keep the number of tablet in a relatively appropriate range.

### New Functions

Add the new function `width_bucket`. 

Reference: [https://doris.apache.org/zh-CN/docs/dev/sql-manual/sql-functions/width-bucket/#description](https://doris.apache.org/zh-CN/docs/dev/sql-manual/sql-functions/width-bucket/#description)

# Behavior Changes

- Disable BE's page cache by default: `disable_storage_page_cache=true`

Turn off this configuration to optimize memory usage and reduce the risk of memory OOM.
But it will reduce the query latency of some small queries.
If you are sensitive to query latency, or have high concurrency and small query scenarios, you can configure *disable_storage_page_cache=false* to enable page cache again.

- Add new session variable `group_by_and_having_use_alias_first`, used to control whether the group and having clauses use alias.

Reference: [https://doris.apache.org/docs/dev/advanced/variables](https://doris.apache.org/docs/dev/advanced/variables)

# Improvement

### Compaction

- Support `Vertical Compaction`. To optimize the compaction overhead and efficiency of wide tables.

- Support `Segment ompaction`. Fix -238 and -235 issues with high frequency imports.

### Lakehouse

- Hive Catalog can be compatible with Hive version 1/2/3

- Hive Catalog can access JuiceFS based HDFS with Broker.

- Iceberg Catalog Support Hive Metastore and Rest Catalog type.

- ES Catalog support _id column mapping.

- Optimize Iceberg V2 read performance with large number of delete rows.

- Support for reading Iceberg tables after Schema Evolution

- Parquet Reader handles column name case correctly.

### Other

- Support for accessing Hadoop KMS-encrypted HDFS.

- Support to cancel the Export export task in progress.

- Optimize the performance of `explode_split` with 1x.

- Optimize the read performance of nullable columns with 3x.

- Optimize some problems of Memtracker, improve memory management accuracy, and optimize memory application.



# Bug Fix
 
- Fixed memory leak when loading data with Doris Flink Connector.

- Fixed the possible thread scheduling problem of BE and reduce the `Fragment sent timeout` error caused by BE thread exhaustion.

- Fixed various correctness and precision issues of column type datetimev2/decimalv3.

- Fixed the problem data correctness issue with Unique Key Merge-on-Read table.

- Fixed various known issues with the Light Schema Change feature.

- Fixed various data correctness issues of bitmap type Runtime Filter.

- Fixed the problem of poor reading performance of csv reader introduced in version 1.2.1.

- Fixed the problem of BE OOM caused by Spark Load data download phase. 

- Fixed possible metadata compatibility issues when upgrading from version 1.1 to version 1.2. 

- Fixed the metadata problem when creating JDBC Catalog with Resource.

- Fixed the problem of high CPU usage caused by load operation.

- Fixed the problem of FE OOM caused by a large number of failed Broker Load jobs.

- Fixed the problem of precision loss when loading floating-point types.

- Fixed the problem of memory leak when useing 2PC stream load

# Other

Add metrics to view the total rowset and segment numbers on BE

- doris_be_all_rowsets_num and doris_be_all_segments_num


# Big Thanks

Thanks to ALL who contributed to this release!


@adonis0147

@AshinGau

@BePPPower

@BiteTheDDDDt

@ByteYue

@caiconghui

@cambyzju

@chenlinzhong

@DarvenDuan

@dataroaring

@Doris-Extras

@dutyu

@englefly

@freemandealer

@Gabriel39

@HappenLee

@Henry2SS

@htyoung

@isHuangXin

@JackDrogon

@jacktengg

@Jibing-Li

@kaka11chen

@Kikyou1997

@Lchangliang

@LemonLiTree

@liaoxin01

@liqing-coder

@luozenglin

@morningman

@morrySnow

@mrhhsg

@nextdreamblue

@qidaye

@qzsee

@spaces-X

@stalary


@starocean999

@weizuo93

@wsjz

@xiaokang

@xinyiZzz

@xy720

@yangzhg

@yiguolei

@yixiutt

@Yukang-Lian

@Yulei-Yang

@zclllyybb

@zddr

@zhangstar333

@zhannngchen

@zy-kkk






