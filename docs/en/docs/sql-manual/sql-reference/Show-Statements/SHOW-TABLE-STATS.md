---
{
    "title": "SHOW-TABLE-STATS",
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

## SHOW-TABLE-STATS

### Name

SHOW TABLE STATS

### Description

Use `SHOW TABLE STATS` to view an overview of statistics collection for a table.

Syntax:

```SQL
SHOW TABLE STATS table_name;
```

Where:

- table_name: The target table name. It can be in the format `db_name.table_name`.

Output:

| Column Name           | Description      |
| :--------------------- | :--------------- |
| `updated_rows`        | Updated rows since the last ANALYZE |
| `query_times`         | Reserved column for recording the number of times the table was queried in future versions |
| `row_count`           | Number of rows (does not reflect the exact number of rows at the time of command execution) |
| `updated_time`        | Last update time |
| `columns`             | Columns for which statistics information has been collected |

Here's an example:

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

### Keywords

SHOW, TABLE, STATS
