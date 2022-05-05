---
{
    "title": "SHOW ALTER TABLE MATERIALIZED VIEW",
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

## SHOW ALTER TABLE MATERIALIZED VIEW

### Name

SHOW ALTER TABLE MATERIALIZED VIEW

### Description

This command is used to view the execution of the Create Materialized View job submitted through the [CREATE-MATERIALIZED-VIEW](../../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-MATERIALIZED-VIEW.html) statement.

> This statement is equivalent to `SHOW ALTER TABLE ROLLUP`;

```sql
SHOW ALTER TABLE MATERIALIZED VIEW
[FROM database]
[WHERE]
[ORDER BY]
[LIMIT OFFSET]
````

- database: View jobs under the specified database. If not specified, the current database is used.
- WHERE: You can filter the result column, currently only the following columns are supported:
  - TableName: Only equal value filtering is supported.
  - State: Only supports equivalent filtering.
  - Createtime/FinishTime: Support =, >=, <=, >, <, !=
- ORDER BY: The result set can be sorted by any column.
- LIMIT: Use ORDER BY to perform page-turning query.

Return result description:

```sql
mysql> show alter table materialized view\G
**************************** 1. row ******************** ******
          JobId: 11001
      TableName: tbl1
     CreateTime: 2020-12-23 10:41:00
     FinishTime: NULL
  BaseIndexName: tbl1
RollupIndexName: r1
       RollupId: 11002
  TransactionId: 5070
          State: WAITING_TXN
            Msg:
       Progress: NULL
        Timeout: 86400
1 row in set (0.00 sec)
````

- `JobId`: Job unique ID.

- `TableName`: base table name

- `CreateTime/FinishTime`: Job creation time and end time.

- `BaseIndexName/RollupIndexName`: Base table name and materialized view name.

- `RollupId`: The unique ID of the materialized view.

- `TransactionId`: See the description of the State field.

- `State`: job status.

  - PENDING: The job is in preparation.

  - WAITING_TXN:

    Before officially starting to generate materialized view data, it will wait for the current running import transaction on this table to complete. And the `TransactionId` field is the current waiting transaction ID. When all previous imports for this ID are complete, the job will actually start.

  - RUNNING: The job is running.

  - FINISHED: The job ran successfully.

  - CANCELLED: The job failed to run.

- `Msg`: error message

- `Progress`: job progress. The progress here means `completed tablets/total tablets`. Materialized views are created at tablet granularity.

- `Timeout`: Job timeout, in seconds.

### Example

1. View the materialized view jobs under the database example_db

   ```sql
   SHOW ALTER TABLE MATERIALIZED VIEW FROM example_db;
   ````

### Keywords

    SHOW, ALTER, TABLE, MATERIALIZED, VIEW

### Best Practice

