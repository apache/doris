---
{
    "title": "CLEAN-QUERY-STATS",
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

## CLEAN-QUERY-STATS

### Name

<version since="dev">
CLEAN QUERY STATS
</version>

### Description

This statement is used to clear query statistics

grammar：

```sql
CLEAN [ALL| DATABASE | TABLE] QUERY STATS [[FOR db_name]|[FROM|IN] table_name]];
```

Remarks：

1. If ALL is specified, all query statistics are cleared, including database and table, admin privilege is needed
2. If DATABASE is specified, the query statistics of the specified database are cleared, alter privilege for this database is needed
3. If TABLE is specified, the query statistics of the specified table are cleared, alter privilege for this table is needed

### Example

1. Clear all statistics
2. 
    ```sql
    clean all query stats;
    ```

2. Clear the specified database statistics

    ```sql
    clean database query stats for test_query_db;
    ```
3. Clear the specified table statistics

    ```sql
    clean table query stats from test_query_db.baseall;
    ```

### Keywords

    CLEAN, QUERY, STATS

### Best Practice

