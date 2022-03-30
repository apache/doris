---
{
    "title": "DROP MATERIALIZED VIEW",
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

# DROP MATERIALIZED VIEW

## description
    This statement is used to delete a materialized view. Synchronization syntax

syntax:

    ```
    DROP MATERIALIZED VIEW [IF EXISTS] mv_name ON table_name
    ```

1. IF EXISTS
	If the materialized view does not exist, doris will not throw an error. If this keyword is not declared, an error will be reported if the materialized view does not exist.
Ranch

2. mv_name
	The name of the materialized view to be deleted. Required.

3. Table_name
	Name of the table to which the materialized view to be deleted belongs. Required.

## example

Table structure is

```
mysql> desc all_type_table all;
+----------------+-------+----------+------+-------+---------+-------+
| IndexName      | Field | Type     | Null | Key   | Default | Extra |
+----------------+-------+----------+------+-------+---------+-------+
| all_type_table | k1    | TINYINT  | Yes  | true  | N/A     |       |
|                | k2    | SMALLINT | Yes  | false | N/A     | NONE  |
|                | k3    | INT      | Yes  | false | N/A     | NONE  |
|                | k4    | BIGINT   | Yes  | false | N/A     | NONE  |
|                | k5    | LARGEINT | Yes  | false | N/A     | NONE  |
|                | k6    | FLOAT    | Yes  | false | N/A     | NONE  |
|                | k7    | DOUBLE   | Yes  | false | N/A     | NONE  |
|                |       |          |      |       |         |       |
| k1_sumk2       | k1    | TINYINT  | Yes  | true  | N/A     |       |
|                | k2    | SMALLINT | Yes  | false | N/A     | SUM   |
+----------------+-------+----------+------+-------+---------+-------+
```

1. Drop the materialized view named k1_sumk2 of the table all_type_table

	```
	drop materialized view k1_sumk2 on all_type_table;
	```
	Table structure after materialized view is deleted as following:

	```
+----------------+-------+----------+------+-------+---------+-------+
| IndexName      | Field | Type     | Null | Key   | Default | Extra |
+----------------+-------+----------+------+-------+---------+-------+
| all_type_table | k1    | TINYINT  | Yes  | true  | N/A     |       |
|                | k2    | SMALLINT | Yes  | false | N/A     | NONE  |
|                | k3    | INT      | Yes  | false | N/A     | NONE  |
|                | k4    | BIGINT   | Yes  | false | N/A     | NONE  |
|                | k5    | LARGEINT | Yes  | false | N/A     | NONE  |
|                | k6    | FLOAT    | Yes  | false | N/A     | NONE  |
|                | k7    | DOUBLE   | Yes  | false | N/A     | NONE  |
+----------------+-------+----------+------+-------+---------+-------+
	```

2. Delete a non-existing materialized view in the table all_type_table

	```
	drop materialized view k1_k2 on all_type_table;
ERROR 1064 (HY000): errCode = 2, detailMessage = Materialized view [k1_k2] does not exist in table [all_type_table]
	```
	
	The delete request directly reports an error

3. Delete the materialized view k1_k2 in the table all_type_table. Materialized view does not exist and no error is reported.

	```
	drop materialized view if exists k1_k2 on all_type_table;
Query OK, 0 rows affected (0.00 sec)
	```

	If it exists, it will be deleted; If it does not exist, no error will be reported.

## keyword
  DROP, MATERIALIZED, VIEW
