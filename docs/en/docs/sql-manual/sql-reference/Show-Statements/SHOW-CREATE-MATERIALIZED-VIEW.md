---
{
    "title": "SHOW-CREATE-MATERIALIZED-VIEW",
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

## SHOW-CREATE-MATERIALIZED-VIEW

### Name

SHOW CREATE MATERIALIZED VIEW

### Description

This statement is used to query statements that create materialized views.

grammar：

```sql
SHOW CREATE MATERIALIZED VIEW mv_name ON table_name
```

1. mv_name:
   Materialized view name. required.

2. table_name:
   The table name of materialized view. required.

### Example

Create materialized view

```sql
create materialized view id_col1 as select id,col1 from table3;
```

Return after query

```sql
mysql> show create materialized view id_col1 on table3;
+-----------+----------+----------------------------------------------------------------+
| TableName | ViewName | CreateStmt                                                     |
+-----------+----------+----------------------------------------------------------------+
| table3    | id_col1  | create materialized view id_col1 as select id,col1 from table3 |
+-----------+----------+----------------------------------------------------------------+
1 row in set (0.00 sec)
```

### Keywords

    SHOW, MATERIALIZED, VIEW

### Best Practice

