---
{
    "title": "CANCEL ALTER",
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

# CANCEL ALTER
## Description
This statement is used to undo an ALTER operation.
1. 撤销 ALTER TABLE COLUMN 操作
Grammar:
CANCEL ALTER TABLE COLUMN
FROM db_name.table_name

2. 撤销 ALTER TABLE ROLLUP 操作
Grammar:
CANCEL ALTER TABLE ROLLUP
FROM db_name.table_name

3. batch cancel rollup by job id
    Grammar:
        CANCEL ALTER TABLE ROLLUP
                FROM db_name.table_name (jobid,...)
    Note:
        Batch cancel rollup job is an async operation, use `show alter table rollup` to see whether it executes successfully

2. OTHER CLUSTER
Grammar:
(To be realized...


## example
[CANCEL ALTER TABLE COLUMN]
1. 撤销针对 my_table 的 ALTER COLUMN 操作。
CANCEL ALTER TABLE COLUMN
FROM example_db.my_table;

[CANCEL ALTER TABLE ROLLUP]
1. 撤销 my_table 下的 ADD ROLLUP 操作。
CANCEL ALTER TABLE ROLLUP
FROM example_db.my_table;

[CANCEL ALTER TABLE ROLLUP]
1. cancel rollup alter job by job id
CANCEL ALTER TABLE ROLLUP
FROM example_db.my_table (12801,12802);

## keyword
CANCEL,ALTER,TABLE,COLUMN,ROLLUP

