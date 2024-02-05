---
{
    "title": "SHOW-COLUMN-STATS",
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

## SHOW-COLUMN-STATS

### Name

SHOW COLUMN STATS

### Description

通过 `SHOW COLUMN STATS` 来查看列的各项统计数据。

语法如下：

```SQL
SHOW COLUMN [cached] STATS table_name [ (column_name [, ...]) ];
```

其中：

- cached: 展示当前FE内存缓存中的统计信息。
- table_name: 收集统计信息的目标表。可以是  `db_name.table_name`  形式。
- column_name: 指定的目标列，必须是  `table_name`  中存在的列，多个列名称用逗号分隔。

下面是一个例子：

```sql
mysql> show column stats lineitem(l_tax)\G;
*************************** 1. row ***************************
  column_name: l_tax
        count: 6001215.0
          ndv: 9.0
     num_null: 0.0
    data_size: 4.800972E7
avg_size_byte: 8.0
          min: 0.00
          max: 0.08
       method: FULL
         type: FUNDAMENTALS
      trigger: MANUAL
  query_times: 0
 updated_time: 2023-11-07 11:00:46

```

### Keywords

SHOW, COLUMN, STATS
