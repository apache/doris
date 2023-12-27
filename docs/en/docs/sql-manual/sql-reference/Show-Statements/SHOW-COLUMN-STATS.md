---
{
    "title": "SHOW-COLUMN-STATS",
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

## SHOW-COLUMN-STATS

### Name

SHOW COLUMN STATS

### Description

Use `SHOW COLUMN STATS` to view various statistics data for columns.

Syntax:

```SQL
SHOW COLUMN [cached] STATS table_name [ (column_name [, ...]) ];
```

Where:

- cached: Show statistics information in the current FE memory cache.
- table_name: The target table for collecting statistics. It can be in the format `db_name.table_name`.
- column_name: Specifies the target column, which must be an existing column in `table_name`. You can specify multiple column names separated by commas.

Here's an example:

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

SHOW, TABLE, STATS
