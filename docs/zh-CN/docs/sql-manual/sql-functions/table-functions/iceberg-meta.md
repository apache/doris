---
{
"title": "ICEBERG_META",
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

## iceberg_meta

### Name

<version since="1.2">

iceberg_meta

</version>

### description

iceberg_meta表函数（table-valued-function,tvf），可以用于读取iceberg表的各类元数据信息，如操作历史、生成的快照、文件元数据等。

#### syntax
```sql
iceberg_meta(
  "table" = "ctl.db.tbl", 
  "query_type" = "snapshots"
  ...
  );
```

**参数说明**

iceberg_meta表函数 tvf中的每一个参数都是一个 `"key"="value"` 对。
相关参数：
- `table`： (必填) 完整的表名，需要按照目录名.库名.表名的格式，填写需要查看的iceberg表名。
- `query_type`： (必填) 想要查看的元数据类型，目前仅支持snapshots。

### Example

读取并访问iceberg表格式的snapshots元数据。

```sql
select * from iceberg_meta("table" = "ctl.db.tbl", "query_type" = "snapshots");

```

可以配合`desc function`使用

```sql
desc function iceberg_meta("table" = "ctl.db.tbl", "query_type" = "snapshots");
```

### Keywords

    iceberg_meta, table-valued-function, tvf

### Best Prac

查看iceberg表的snapshots

```sql
select * from iceberg_meta("table" = "iceberg_ctl.test_db.test_tbl", "query_type" = "snapshots");
+------------------------+----------------+---------------+-----------+-------------------+
|      committed_at      |  snapshot_id   |   parent_id   | operation |   manifest_list   |
+------------------------+----------------+---------------+-----------+-------------------+
|  2022-09-20 11:14:29   |  64123452344   |       -1      |  append   | hdfs:/path/to/m1  |
|  2022-09-21 10:36:35   |  98865735822   |  64123452344  | overwrite | hdfs:/path/to/m2  |
|  2022-09-21 21:44:11   |  51232845315   |  98865735822  | overwrite | hdfs:/path/to/m3  |
+------------------------+----------------+---------------+-----------+-------------------+
```

根据snapshot_id字段筛选

```sql
select * from iceberg_meta("table" = "iceberg_ctl.test_db.test_tbl", "query_type" = "snapshots") 
where snapshot_id = 98865735822;
+------------------------+----------------+---------------+-----------+-------------------+
|      committed_at      |  snapshot_id   |   parent_id   | operation |   manifest_list   |
+------------------------+----------------+---------------+-----------+-------------------+
|  2022-09-21 10:36:35   |  98865735822   |  64123452344  | overwrite | hdfs:/path/to/m2  |
+------------------------+----------------+---------------+-----------+-------------------+
```
