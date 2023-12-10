---
{
    "title": "ANALYZE",
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

## ANALYZE

### Name

<version since="2.0"></version>

ANALYZE

### Description

该语句用于收集各列的统计信息。

```sql
ANALYZE < TABLE | DATABASE table_name | db_name > 
    [ (column_name [, ...]) ]
    [ [ WITH SYNC ] [ WITH SAMPLE PERCENT | ROWS ] ];
```

- table_name: 指定的目标表。可以是  `db_name.table_name`  形式。
- column_name: 指定的目标列。必须是  `table_name`  中存在的列，多个列名称用逗号分隔。
- sync：同步收集统计信息。收集完后返回。若不指定则异步执行并返回JOB ID。
- sample percent | rows：抽样收集统计信息。可以指定抽样比例或者抽样行数。

### Example

对一张表按照10%的比例采样收集统计数据：

```sql
ANALYZE TABLE lineitem WITH SAMPLE PERCENT 10;
```

对一张表按采样10万行收集统计数据

```sql
ANALYZE TABLE lineitem WITH SAMPLE ROWS 100000;
```

### Keywords

ANALYZE
