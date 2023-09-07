---
{
    "title": "Release 1.2.3",
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

### JDBC Catalog 

- Support connecting to Doris clusters through JDBC Catalog.

Currently, Jdbc Catalog only support to use 5.x version of JDBC jar package to connect another Doris database. If you use 8.x version of JDBC jar package, the data type of column may not be matched.

Reference: [https://doris.apache.org/docs/dev/lakehouse/multi-catalog/jdbc/#doris](https://doris.apache.org/docs/dev/lakehouse/multi-catalog/jdbc/#doris)

- Support to synchronize only the specified database through the `only_specified_database` attribute.

- Support synchronizing table names in the form of lowercase through `lower_case_table_names` to solve the problem of case sensitivity of table names.

Reference: [https://doris.apache.org/docs/dev/lakehouse/multi-catalog/jdbc](https://doris.apache.org/docs/dev/lakehouse/multi-catalog/jdbc)

- Optimize the read performance of JDBC Catalog.

### Elasticsearch Catalog

- Support Array type mapping.

- Support whether to push down the like expression through the `like_push_down` attribute to control the CPU overhead of the ES cluster.

Reference: [https://doris.apache.org/docs/dev/lakehouse/multi-catalog/es](https://doris.apache.org/docs/dev/lakehouse/multi-catalog/es)

### Hive Catalog

- Support Hive table default partition `_HIVE_DEFAULT_PARTITION_`.

- Hive Metastore metadata automatic synchronization supports notification event in compressed format.

### Dynamic Partition Improvement

- Dynamic partition supports specifying the `storage_medium` parameter to control the storage medium of the newly added partition.

Reference: [https://doris.apache.org/docs/dev/advanced/partition/dynamic-partition](https://doris.apache.org/docs/dev/advanced/partition/dynamic-partition)


### Optimize BE's Threading Model

- Optimize BE's threading model to avoid stability problems caused by frequent thread creation and destroy.

# Bugfix

- Fixed issues with Merge-On-Write Unique Key tables.

- Fixed compaction related issues.

- Fixed some delete statement issues causing data errors.

- Fixed several query execution errors.

- Fixed the problem of using JDBC catalog to cause BE crash on some operating system.

- Fixed Multi-Catalog issues.

- Fixed memory statistics and optimization issues.

- Fixed decimalV3 and date/datetimev2 related issues.

- Fixed load transaction stability issues.

- Fixed light-weight schema change issues.

- Fixed the issue of using `datetime` type for batch partition creation.

- Fixed the problem that a large number of failed broker loads would cause the FE memory usage to be too high.

- Fixed the problem that stream load cannot be canceled after dropping the table.

- Fixed querying `information_schema` timeout in some cases.

- Fixed the problem of BE crash caused by concurrent data export using `select outfile`.

- Fixed transactional insert operation memory leak.

- Fixed several query/load profile issues, and supports direct download of profiles through FE web ui.

- Fixed the problem that the BE tablet GC thread caused the IO util to be too high.

- Fixed the problem that the commit offset is inaccurate in Kafka routine load.

