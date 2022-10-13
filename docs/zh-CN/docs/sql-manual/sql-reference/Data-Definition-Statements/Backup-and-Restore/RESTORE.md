---
{
    "title": "RESTORE",
    "language": "zh-CN"
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

## RESTORE

### Name

RESTORE

### Description

该语句用于将之前通过 BACKUP 命令备份的数据，恢复到指定数据库下。该命令为异步操作。提交成功后，需通过 SHOW RESTORE 命令查看进度。仅支持恢复 OLAP 类型的表。

语法：

```sql
RESTORE SNAPSHOT [db_name].{snapshot_name}
FROM `repository_name`
[ON|EXCLUDE] (
    `table_name` [PARTITION (`p1`, ...)] [AS `tbl_alias`],
    ...
)
PROPERTIES ("key"="value", ...);
```

说明：
- 同一数据库下只能有一个正在执行的 BACKUP 或 RESTORE 任务。
- ON 子句中标识需要恢复的表和分区。如果不指定分区，则默认恢复该表的所有分区。所指定的表和分区必须已存在于仓库备份中。
- EXCLUDE 子句中标识不需要恢复的表和分区。除了所指定的表或分区之外仓库中所有其他表的所有分区将被恢复。
- 可以通过 AS 语句将仓库中备份的表名恢复为新的表。但新表名不能已存在于数据库中。分区名称不能修改。
- 可以将仓库中备份的表恢复替换数据库中已有的同名表，但须保证两张表的表结构完全一致。表结构包括：表名、列、分区、Rollup等等。
- 可以指定恢复表的部分分区，系统会检查分区 Range 或者 List 是否能够匹配。
- PROPERTIES 目前支持以下属性：
  -  "backup_timestamp" = "2018-05-04-16-45-08"：指定了恢复对应备份的哪个时间版本，必填。该信息可以通过 `SHOW SNAPSHOT ON repo;` 语句获得。
  - "replication_num" = "3"：指定恢复的表或分区的副本数。默认为3。若恢复已存在的表或分区，则副本数必须和已存在表或分区的副本数相同。同时，必须有足够的 host 容纳多个副本。
  - "reserve_replica" = "true"：默认为 false。当该属性为 true 时，会忽略 replication_num 属性，恢复的表或分区的副本数将与备份之前一样。支持多个表或表内多个分区有不同的副本数。
  - "reserve_dynamic_partition_enable" = "true"：默认为 false。当该属性为 true 时，恢复的表会保留该表备份之前的'dynamic_partition_enable'属性值。该值不为true时，则恢复出来的表的'dynamic_partition_enable'属性值会设置为false。
  - "timeout" = "3600"：任务超时时间，默认为一天。单位秒。
  - "meta_version" = 40：使用指定的 meta_version 来读取之前备份的元数据。注意，该参数作为临时方案，仅用于恢复老版本 Doris 备份的数据。最新版本的备份数据中已经包含 meta version，无需再指定。     

### Example

1. 从 example_repo 中恢复备份 snapshot_1 中的表 backup_tbl 到数据库 example_db1，时间版本为 "2018-05-04-16-45-08"。恢复为 1 个副本：

```sql
RESTORE SNAPSHOT example_db1.`snapshot_1`
FROM `example_repo`
ON ( `backup_tbl` )
PROPERTIES
(
    "backup_timestamp"="2018-05-04-16-45-08",
    "replication_num" = "1"
);
```

2. 从 example_repo 中恢复备份 snapshot_2 中的表 backup_tbl 的分区 p1,p2，以及表 backup_tbl2 到数据库 example_db1，并重命名为 new_tbl，时间版本为 "2018-05-04-17-11-01"。默认恢复为 3 个副本：

```sql
RESTORE SNAPSHOT example_db1.`snapshot_2`
FROM `example_repo`
ON
(
    `backup_tbl` PARTITION (`p1`, `p2`),
    `backup_tbl2` AS `new_tbl`
)
PROPERTIES
(
    "backup_timestamp"="2018-05-04-17-11-01"
);
```

3. 从 example_repo 中恢复备份 snapshot_3 中除了表 backup_tbl 的其他所有表到数据库 example_db1，时间版本为 "2018-05-04-18-12-18"。

```sql
RESTORE SNAPSHOT example_db1.`snapshot_3`
FROM `example_repo`
EXCLUDE ( `backup_tbl` )
PROPERTIES
(
    "backup_timestamp"="2018-05-04-18-12-18"
);
```

### Keywords

    RESTORE

### Best Practice

1. 同一数据库下只能有一个正在执行的恢复操作。

2. 可以将仓库中备份的表恢复替换数据库中已有的同名表，但须保证两张表的表结构完全一致。表结构包括：表名、列、分区、物化视图等等。

3. 当指定恢复表的部分分区时，系统会检查分区范围是否能够匹配。

4. 恢复操作的效率：

   在集群规模相同的情况下，恢复操作的耗时基本等同于备份操作的耗时。如果想加速恢复操作，可以先通过设置 `replication_num` 参数，仅恢复一个副本，之后在通过调整副本数 [ALTER TABLE PROPERTY](../../Data-Definition-Statements/Alter/ALTER-TABLE-PROPERTY.md)，将副本补齐。
