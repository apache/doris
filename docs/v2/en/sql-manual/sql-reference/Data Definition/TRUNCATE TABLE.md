---
{
    "title": "TRUNCATE TABLES",
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

# TRUNCATE TABLES
## Description
This statement is used to empty the data of the specified table and partition
Grammar:

TRUNCATE TABLE [db.]tbl[ PARTITION(p1, p2, ...)];

Explain:
1. The statement empties the data, but retains the table or partition.
2. Unlike DELETE, this statement can only empty the specified tables or partitions as a whole, without adding filtering conditions.
3. Unlike DELETE, using this method to clear data will not affect query performance.
4. The data deleted by this operation is not recoverable.
5. When using this command, the table state should be NORMAL, i.e. SCHEMA CHANGE operations are not allowed.

## example

1. Clear the table TBL under example_db

TRUNCATE TABLE example_db.tbl;

2. P1 and P2 partitions of clearing TABLE tbl

TRUNCATE TABLE tbl PARTITION(p1, p2);

## keyword
TRUNCATE,TABLE
