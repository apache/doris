---
{
    "title": "Replace Table",
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

# Replace Table

In version 0.14, Doris supports atomic replacement of two tables.
This operation only applies to OLAP tables.

For partition level replacement operations, please refer to [Temporary Partition Document](../partition/table-temp-partition.html)

## Syntax

```
ALTER TABLE [db.]tbl1 REPLACE WITH tbl2
[PROPERTIES('swap' = 'true')];
```

Replace table `tbl1` with table `tbl2`.

If the `swap` parameter is `true`, after replacement, the data in the table named `tbl1` is the data in the original `tbl2` table. The data in the table named `tbl2` is the data in the original table `tbl1`. That is, the data of the two tables are interchanged.

If the `swap` parameter is `false`, after replacement, the data in the table named `tbl1` is the data in the original `tbl2` table. The table named `tbl2` is dropped.

## Principle

The replacement table function actually turns the following set of operations into an atomic operation.

Suppose you want to replace table A with table B, and `swap` is `true`, the operation is as follows:

1. Rename table B to table A.
2. Rename table A to table B.

If `swap` is `false`, the operation is as follows:

1. Drop table A.
2. Rename table B to table A.

## Notice

1. The `swap` parameter defaults to `true`. That is, the replacement table operation is equivalent to the exchange of two table data.
2. If the `swap` parameter is set to `false`, the replaced table (table A) will be dropped and cannot be recovered.
3. The replacement operation can only occur between two OLAP tables, and the table structure of the two tables is not checked for consistency.
4. The replacement operation will not change the original permission settings. Because the permission check is based on the table name.

## Best Practices

1. Atomic Overwrite Operation

    In some cases, the user wants to be able to rewrite the data of a certain table, but if it is dropped and then imported, there will be a period of time in which the data cannot be viewed. At this time, the user can first use the `CREATE TABLE LIKE` statement to create a new table with the same structure, import the new data into the new table, and replace the old table atomically through the replacement operation to achieve the goal. For partition level atomic overwrite operation, please refer to [Temporary partition document](../partition/table-temp-partition.html)