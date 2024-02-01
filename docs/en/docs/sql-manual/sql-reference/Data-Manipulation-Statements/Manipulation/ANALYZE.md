---
{
    "title": "ANALYZE",
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

## ANALYZE

### Name

<version since="2.0"></version>

ANALYZE

### Description

This statement is used to collect statistical information for various columns.

```sql
ANALYZE < TABLE | DATABASE table_name | db_name > 
    [ (column_name [, ...]) ]
    [ [ WITH SYNC ] [ WITH SAMPLE PERCENT | ROWS ] ];
```

- `table_name`: The specified target table. It can be in the format `db_name.table_name`.
- `column_name`: The specified target column. It must be an existing column in `table_name`. You can specify multiple column names separated by commas.
- `sync`: Collect statistics synchronously. Returns after collection. If not specified, it executes asynchronously and returns a JOB ID.
- `sample percent | rows`: Collect statistics with sampling. You can specify a sampling percentage or a number of sampling rows.

### Example

Collect statistical data for a table with a 10% sampling rate:

```sql
ANALYZE TABLE lineitem WITH SAMPLE PERCENT 10;
```

Collect statistical data for a table with a sample of 100,000 rows:

```sql
ANALYZE TABLE lineitem WITH SAMPLE ROWS 100000;
```

### Keywords

ANALYZE