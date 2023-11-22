---
{
    "title": "SHOW-TABLE-STATS",
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

## SHOW-TABLE-STATS

### Name

SHOW TABLE STATS

### Description

通过 `SHOW TABLE STATS` 查看表的统计信息收集概况。

语法如下：

```SQL
SHOW TABLE STATS table_name;
```

其中：

- table_name: 目标表表名。可以是  `db_name.table_name`  形式。

输出：

| 列名                | 说明                   |
| :------------------ | :--------------------- |
|`updated_rows`|自上次ANALYZE以来该表的更新行数|
|`query_times`|保留列，后续版本用以记录该表查询次数|
|`row_count`| 行数（不反映命令执行时的准确行数）|
|`updated_time`| 上次更新时间|
|`columns`| 收集过统计信息的列|
|`trigger`|触发方式|

下面是一个例子：

```sql
mysql> show table stats lineitem \G;
*************************** 1. row ***************************
updated_rows: 0
 query_times: 0
   row_count: 6001215
updated_time: 2023-11-07
     columns: [l_returnflag, l_receiptdate, l_tax, l_shipmode, l_suppkey, l_shipdate, l_commitdate, l_partkey, l_orderkey, l_quantity, l_linestatus, l_comment, l_extendedprice, l_linenumber, l_discount, l_shipinstruct]
     trigger: MANUAL
```

<br/>

<br/>

### Keywords

SHOW, TABLE, STATS
