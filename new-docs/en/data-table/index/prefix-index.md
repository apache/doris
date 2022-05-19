---
{
    "title": "prefix index",
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

# Prefix Index

## Basic concept

Unlike traditional database designs, Doris does not support creating indexes on arbitrary columns. OLAP databases with MPP architectures such as Doris usually process large amounts of data by improving concurrency.

Essentially, Doris' data is stored in a data structure similar to SSTable (Sorted String Table). This structure is an ordered data structure that can be sorted and stored according to the specified column. In this data structure, it is very efficient to use the sorted column as a condition to search.

In the three data models of Aggregate, Unique and Duplicate. The underlying data storage is sorted and stored according to the columns specified in the AGGREGATE KEY, UNIQUE KEY and DUPLICATE KEY in the respective table creation statements.

The prefix index, that is, on the basis of sorting, implements an index method to quickly query data according to a given prefix column.

## Example

We use the first 36 bytes of a row of data as the prefix index of this row of data. Prefix indexes are simply truncated when a VARCHAR type is encountered. We give an example:

1. The prefix index of the following table structure is user_id(8 Bytes) + age(4 Bytes) + message(prefix 20 Bytes).

   | ColumnName     | Type         |
   | -------------- | ------------ |
   | user_id        | BIGINT       |
   | age            | INT          |
   | message        | VARCHAR(100) |
   | max_dwell_time | DATETIME     |
   | min_dwell_time | DATETIME     |

2. The prefix index of the following table structure is user_name(20 Bytes). Even if it does not reach 36 bytes, because VARCHAR is encountered, it is directly truncated and will not continue further.

   | ColumnName     | Type         |
   | -------------- | ------------ |
   | user_name      | VARCHAR(20)  |
   | age            | INT          |
   | message        | VARCHAR(100) |
   | max_dwell_time | DATETIME     |
   | min_dwell_time | DATETIME     |

When our query condition is the prefix of the prefix index, the query speed can be greatly accelerated. For example, in the first example, we execute the following query:

```sql
SELECT * FROM table WHERE user_id=1829239 and age=20；
```

This query will be much more efficient than the following query:

```sql
SELECT * FROM table WHERE age=20；
```

Therefore, when building a table, choosing the correct column order can greatly improve query efficiency.

## Adjust prefix index by ROLLUP

Because the column order has been specified when the table is created, there is only one prefix index for a table. This may not be efficient for queries that use other columns that cannot hit the prefix index as conditions. Therefore, we can artificially adjust the column order by creating a ROLLUP. For details, please refer to [ROLLUP](../hit-the-rollup.md).