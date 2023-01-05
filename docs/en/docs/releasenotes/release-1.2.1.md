---
{
    "title": "Release 1.2.1",
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

# Improvement

### Supports new type DecimalV3

DecimalV3, which supports higher precision and better performance, has the following advantages over past versions.

- Larger representable range, the range of values are significantly expanded, and the valid number range [1,38].

- Higher performance, adaptive adjustment of the storage space occupied according to different precision.

- More complete precision derivation support, for different expressions, different precision derivation rules are applied to the accuracy of the result.

[DecimalV3](https://doris.apache.org/docs/dev/sql-manual/sql-reference/Data-Types/DECIMALV3/)

### Support Iceberg V2

Support Iceberg V2 (only Position Delete is supported, Equality Delete will be supported in subsequent versions).

Tables in Iceberg V2 format can be accessed through the Multi-Catalog feature.

### Support OR condition to IN

Support converting  OR condition to IN condition, which can improve the execution efficiency in some scenarios.[#15437](https://github.com/apache/doris/pull/15437) [#12872](https://github.com/apache/doris/pull/12872)

### Optimize the import and query performance of JSONB type

Optimize the import and query performance of JSONB type. [#15219](https://github.com/apache/doris/pull/15219)  [#15219](https://github.com/apache/doris/pull/15219)

### Stream load supports quoted csv data

Search trim_double_quotes in Document:[https://doris.apache.org/en/docs/dev/sql-manual/sql-reference/Data-Manipulation-Statements/Load/STREAM-LOAD](https://doris.apache.org/en/docs/dev/sql-manual/sql-reference/Data-Manipulation-Statements/Load/STREAM-LOAD)

### Broker supports Tencent Cloud CHDFS and Baidu Cloud BOS, AFS

Data on CHDFS, BOS, and AFS can be accessed through Broker. [#15297](https://github.com/apache/doris/pull/15297) [#15448](https://github.com/apache/doris/pull/15448)

### New function

Add function `substring_index`. [#15373](https://github.com/apache/doris/pull/15373)

# Bug Fix

- In some cases, after upgrading from version 1.1 to version 1.2, the user permission information will be lost. [#15144](https://github.com/apache/doris/pull/15144)

- Fix the problem that the partition value is wrong when using datev2/datetimev2 type for partitioning. [#15094](https://github.com/apache/doris/pull/15094)

- Bug fixes for a large number of released features. For a complete list see: [PR List](https://github.com/apache/doris/pulls?q=is%3Apr+label%3Adev%2F1.2.1-merged+is%3Aclosed)

# Upgrade Notice

### Known Issues

- Do not use JDK11 as the runtime JDK of BE, it will cause BE Crash.
- The reading performance of the csv format in this version has declined, which will affect the import and reading efficiency of the csv format. We will fix it as soon as possible in the next three-digit version

### Behavior Changed

- The default value of the BE configuration item `high_priority_flush_thread_num_per_store` is changed from 1 to 6, to improve the write efficiency of Routine Load. (https://github.com/apache/doris/pull/14775)

- The default value of the FE configuration item `enable_new_load_scan_node` is changed to true. Import tasks will be performed using the new File Scan Node. No impact on users.[#14808](https://github.com/apache/doris/pull/14808)

- Delete the FE configuration item `enable_multi_catalog`. The Multi-Catalog function is enabled by default.

- The vectorized execution engine is forced to be enabled by default.[#15213](https://github.com/apache/doris/pull/15213)

The session variable enable_vectorized_engine will no longer take effect. Enabled by default.

To make it valid again, set the FE configuration item `disable_enable_vectorized_engine` to false, and restart FE to make `enable_vectorized_engine` valid again.


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

@dataroaring

@Doris-Extras

@dutyu

@eldenmoon

@englefly

@freemandealer

@Gabriel39

@HappenLee

@Henry2SS

@hf200012

@jacktengg

@Jibing-Li

@Kikyou1997

@liaoxin01

@luozenglin

@morningman

@morrySnow

@mrhhsg

@nextdreamblue

@qidaye

@spaces-X

@starocean999

@wangshuo128

@weizuo93

@wsjz

@xiaokang

@xinyiZzz

@xutaoustc

@yangzhg

@yiguolei

@yixiutt

@Yulei-Yang

@yuxuan-luo

@zenoyang

@zhangstar333

@zhannngchen

@zhengshengjun






