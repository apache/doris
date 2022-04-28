---
{
    "title": "BACKUP",
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

## BACKUP

### Name

BACKUP

### Description

该语句用于备份指定数据库下的数据。该命令为异步操作。提交成功后，需通过 SHOW BACKUP 命令查看进度。仅支持备份 OLAP 类型的表。

语法：

```sql
BACKUP SNAPSHOT [db_name].{snapshot_name}
TO `repository_name`
[ON|EXCLUDE] (
    `table_name` [PARTITION (`p1`, ...)],
    ...
)
PROPERTIES ("key"="value", ...);
```

说明：

- 同一数据库下只能有一个正在执行的 BACKUP 或 RESTORE 任务。
- ON 子句中标识需要备份的表和分区。如果不指定分区，则默认备份该表的所有分区
- EXCLUDE 子句中标识不需要备份的表和分区。备份除了指定的表或分区之外这个数据库中所有表的所有分区数据。
- PROPERTIES 目前支持以下属性：
  -  "type" = "full"：表示这是一次全量更新（默认）
  - "timeout" = "3600"：任务超时时间，默认为一天。单位秒。          

### Example

1. 全量备份 example_db 下的表 example_tbl 到仓库 example_repo 中：

```sql
BACKUP SNAPSHOT example_db.snapshot_label1
TO example_repo
ON (example_tbl)
PROPERTIES ("type" = "full");
```

2. 全量备份 example_db 下，表 example_tbl 的 p1, p2 分区，以及表 example_tbl2 到仓库 example_repo 中：

```sql
BACKUP SNAPSHOT example_db.snapshot_label2
TO example_repo
ON 
(
    example_tbl PARTITION (p1,p2),
    example_tbl2
);
```

3. 全量备份 example_db 下除了表 example_tbl 的其他所有表到仓库 example_repo 中：

```sql
BACKUP SNAPSHOT example_db.snapshot_label3
TO example_repo
EXCLUDE (example_tbl);
```

### Keywords

```text
BACKUP
```

### Best Practice

1. 同一个数据库下只能进行一个备份操作。

2. 备份操作会备份指定表或分区的基础表及 [物化视图](../../../../advanced/materialized-view.html)，并且仅备份一副本。

3. 备份操作的效率

   备份操作的效率取决于数据量、Compute Node 节点数量以及文件数量。备份数据分片所在的每个 Compute Node 都会参与备份操作的上传阶段。节点数量越多，上传的效率越高。

   文件数据量只涉及到的分片数，以及每个分片中文件的数量。如果分片非常多，或者分片内的小文件较多，都可能增加备份操作的时间。
