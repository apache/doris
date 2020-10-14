---
{
    "title": "INSERT",
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

# INSERT
## Description
### Syntax

```
INSERT INTO table_name
[ PARTITION (p1, ...)]
[ WITH LABEL label]
[ (column [, ...]) ]
[ [ hint [, ...] ] ]
{ VALUES ( { expression | DEFAULT } [, ...] ) [, ...] | query }
```

### Parameters

> tablet_name: Target table for loading data. It can be in the form of `db_name.table_name`.
>
> partitions: Specifies the partitions to be loaded, with multiple partition names separated by commas. The partitions must exist in `table_name`, 
>
> label: Specifies a label for Insert job.
>
> column_name: The specified destination columns must be columns that exists in `table_name`.
>
> expression: The corresponding expression that needs to be assigned to a column.
>
> DEFAULT: Let the corresponding columns use default values
>
> query: A common query whose results are written to the target
>
> hint: Indicators used to indicate `INSERT` execution. ` Both streaming `and default non `streaming'methods use synchronization to complete `INSERT' statement execution
> The non `streaming'mode returns a label after execution to facilitate users to query the imported status through `SHOW LOAD'.

### Note

When the `INSERT'statement is currently executed, the default behavior for data that does not conform to the target table is filtering, such as string length. However, for business scenarios where data is not filtered, the session variable `enable_insert_strict'can be set to `true' to ensure that `INSERT` will not be successfully executed when data is filtered out.

## example

` The test `table contains two columns `c1', `c2'.

1. Import a row of data into the `test` table

```
INSERT INTO test VALUES (1, 2);
INSERT INTO test (c1, c2) VALUES (1, 2);
INSERT INTO test (c1, c2) VALUES (1, DEFAULT);
INSERT INTO test (c1) VALUES (1);
```

The first and second sentences have the same effect. When the target column is not specified, the column order in the table is used as the default target column.
The third and fourth statements express the same meaning, using the default value of `c2'column to complete data import.

2. Import multiline data into the `test` table at one time

```
INSERT INTO test VALUES (1, 2), (3, 2 + 2)
INSERT INTO test (c1, c2) VALUES (1, 2), (3, 2 * 2)
INSERT INTO test (c1) VALUES (1), (3)
Insert in test (C1, C2) values (1, Default), (3, Default)
```

The effect of the first and second statements is the same, and two data are imported into the `test'table at one time.
The effect of the third and fourth statements is known, using the default value of the `c2'column to import two data into the `test' table.


3. Insert into table `test` with a query stmt.

```
INSERT INTO test SELECT * FROM test2
INSERT INTO test (c1, c2) SELECT * from test2
```

4. Insert into table `test` with specified partition and label

```
INSERT INTO test PARTITION(p1, p2) WITH LABEL `label1` SELECT * FROM test2;
INSERT INTO test WITH LABEL `label1` (c1, c2) SELECT * from test2;
```

Asynchronous imports are, in fact, encapsulated asynchronously by a synchronous import. Filling in streaming is as efficient as not filling in * execution.

Since Doris used to import asynchronously, in order to be compatible with the old usage habits, the `INSERT'statement without streaming will still return a label. Users need to view the status of the `label' import job through the `SHOW LOAD command.
## keyword
INSERT
