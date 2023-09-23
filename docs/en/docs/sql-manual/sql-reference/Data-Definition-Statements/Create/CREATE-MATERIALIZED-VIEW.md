---
{
    "title": "CREATE-MATERIALIZED-VIEW",
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

## CREATE-MATERIALIZED-VIEW

### Name

CREATE MATERIALIZED VIEW

### Description

This statement is used to create a materialized view.

This operation is an asynchronous operation. After the submission is successful, you need to view the job progress through [SHOW ALTER TABLE MATERIALIZED VIEW](../../Show-Statements/SHOW-ALTER-TABLE-MATERIALIZED-VIEW.md). After displaying FINISHED, you can use the `desc [table_name] all` command to view the schema of the materialized view.

grammar:

```sql
CREATE MATERIALIZED VIEW < MV name > as < query >
[PROPERTIES ("key" = "value")]
````

illustrate:

- `MV name`: The name of the materialized view, required. Materialized view names for the same table cannot be repeated.

- `query`: The query statement used to construct the materialized view, the result of the query statement is the data of the materialized view. Currently supported query formats are:

  ```sql
  SELECT select_expr[, select_expr ...]
  FROM [Base view name]
  GROUP BY column_name[, column_name ...]
  ORDER BY column_name[, column_name ...]
  ````

  The syntax is the same as the query syntax.

  - `select_expr`: All columns in the schema of the materialized view.
    - Contains at least one single column.
  - `base view name`: The original table name of the materialized view, required.
    - Must be a single table and not a subquery
  - `group by`: The grouping column of the materialized view, optional.
    - If not filled, the data will not be grouped.
  - `order by`: the sorting column of the materialized view, optional.
    - The declaration order of the sort column must be the same as the column declaration order in select_expr.
    - If order by is not declared, the sorting column is automatically supplemented according to the rules. If the materialized view is an aggregate type, all grouping columns are automatically supplemented as sort columns. If the materialized view is of a non-aggregate type, the first 36 bytes are automatically supplemented as the sort column.
    - If the number of auto-supplemented sorts is less than 3, the first three are used as the sort sequence. If query contains a grouping column, the sorting column must be the same as the grouping column.

- properties

  Declare some configuration of the materialized view, optional.

  ````text
  PROPERTIES ("key" = "value", "key" = "value" ...)
  ````

  The following configurations can be declared here:

  ````text
   short_key: The number of sorting columns.
   timeout: The timeout for materialized view construction.
  ````

### Example

Base table structure is

```sql
mysql> desc duplicate_table;
+-------+--------+------+------+---------+-------+
| Field | Type | Null | Key | Default | Extra |
+-------+--------+------+------+---------+-------+
| k1 | INT | Yes | true | N/A | |
| k2 | INT | Yes | true | N/A | |
| k3 | BIGINT | Yes | true | N/A | |
| k4 | BIGINT | Yes | true | N/A | |
+-------+--------+------+------+---------+-------+
````
```sql
create table duplicate_table(
	k1 int null,
	k2 int null,
	k3 bigint null,
	k4 bigint null
)
duplicate key (k1,k2,k3,k4)
distributed BY hash(k4) buckets 3
properties("replication_num" = "1");
```
attention：If the materialized view contains partitioned and distributed columns of the Base table, these columns must be used as key columns in the materialized view

1. Create a materialized view that contains only the columns of the original table (k1, k2)

   ```sql
   create materialized view k2_k1 as
   select k2, k1 from duplicate_table;
   ````

   The schema of the materialized view is as follows, the materialized view contains only two columns k1, k2 without any aggregation

   ````text
   +-------+-------+--------+------+------+ ---------+-------+
   | IndexName | Field | Type | Null | Key | Default | Extra |
   +-------+-------+--------+------+------+ ---------+-------+
   | k2_k1 | k2 | INT | Yes | true | N/A | |
   | | k1 | INT | Yes | true | N/A | |
   +-------+-------+--------+------+------+ ---------+-------+
   ````

2. Create a materialized view with k2 as the sort column

   ```sql
   create materialized view k2_order as
   select k2, k1 from duplicate_table order by k2;
   ````

   The schema of the materialized view is shown in the figure below. The materialized view contains only two columns k2, k1, where k2 is the sorting column without any aggregation.

   ````text
   +-------+-------+--------+------+------- +---------+-------+
   | IndexName | Field | Type | Null | Key | Default | Extra |
   +-------+-------+--------+------+------- +---------+-------+
   | k2_order | k2 | INT | Yes | true | N/A | |
   | | k1 | INT | Yes | false | N/A | NONE |
   +-------+-------+--------+------+------- +---------+-------+
   ````

3. Create a materialized view with k1, k2 grouping and k3 column aggregated by SUM

   ```sql
   create materialized view k1_k2_sumk3 as
   select k1, k2, sum(k3) from duplicate_table group by k1, k2;
   ````

   The schema of the materialized view is shown in the figure below. The materialized view contains two columns k1, k2, sum(k3) where k1, k2 are the grouping columns, and sum(k3) is the sum value of the k3 column grouped by k1, k2.

   Since the materialized view does not declare a sorting column, and the materialized view has aggregated data, the system defaults to supplement the grouping columns k1 and k2 as sorting columns.

   ````text
   +-------+-------+--------+------+------- +---------+-------+
   | IndexName | Field | Type | Null | Key | Default | Extra |
   +-------+-------+--------+------+------- +---------+-------+
   | k1_k2_sumk3 | k1 | INT | Yes | true | N/A | |
   | | k2 | INT | Yes | true | N/A | |
   | | k3 | BIGINT | Yes | false | N/A | SUM |
   +-------+-------+--------+------+------- +---------+-------+
   ````

4. Create a materialized view that removes duplicate rows

   ```sql
   create materialized view deduplicate as
   select k1, k2, k3, k4 from duplicate_table group by k1, k2, k3, k4;
   ````

   The materialized view schema is as shown below. The materialized view contains columns k1, k2, k3, and k4, and there are no duplicate rows.

   ````text
   +-------+-------+--------+------+------- +---------+-------+
   | IndexName | Field | Type | Null | Key | Default | Extra |
   +-------+-------+--------+------+------- +---------+-------+
   | deduplicate | k1 | INT | Yes | true | N/A | |
   | | k2 | INT | Yes | true | N/A | |
   | | k3 | BIGINT | Yes | true | N/A | |
   | | k4 | BIGINT | Yes | true | N/A | |
   +-------+-------+--------+------+------- +---------+-------+
   ````

5. Create a non-aggregate materialized view that does not declare a sort column

   The schema of all_type_table is as follows

   ````
   +-------+--------------+------+-------+---------+- ------+
   | Field | Type | Null | Key | Default | Extra |
   +-------+--------------+------+-------+---------+- ------+
   | k1 | TINYINT | Yes | true | N/A | |
   | k2 | SMALLINT | Yes | true | N/A | |
   | k3 | INT | Yes | true | N/A | |
   | k4 | BIGINT | Yes | true | N/A | |
   | k5 | DECIMAL(9,0) | Yes | true | N/A | |
   | k6 | DOUBLE | Yes | false | N/A | NONE |
   | k7 | VARCHAR(20) | Yes | false | N/A | NONE |
   +-------+--------------+------+-------+---------+- ------+
   ````

   The materialized view contains k3, k4, k5, k6, k7 columns, and does not declare a sort column, the creation statement is as follows:

   ```sql
   create materialized view mv_1 as
   select k3, k4, k5, k6, k7 from all_type_table;
   ````

   The default added sorting column of the system is k3, k4, k5 three columns. The sum of the bytes of these three column types is 4(INT) + 8(BIGINT) + 16(DECIMAL) = 28 < 36. So the addition is that these three columns are used as sorting columns. The schema of the materialized view is as follows, you can see that the key field of the k3, k4, k5 columns is true, that is, the sorting column. The key field of the k6, k7 columns is false, which is a non-sorted column.

   ```sql
   +----------------+-------+--------------+------+-- -----+---------+-------+
   | IndexName | Field | Type | Null | Key | Default | Extra |
   +----------------+-------+--------------+------+-- -----+---------+-------+
   | mv_1 | k3 | INT | Yes | true | N/A | |
   | | k4 | BIGINT | Yes | true | N/A | |
   | | k5 | DECIMAL(9,0) | Yes | true | N/A | |
   | | k6 | DOUBLE | Yes | false | N/A | NONE |
   | | k7 | VARCHAR(20) | Yes | false | N/A | NONE |
   +----------------+-------+--------------+------+-- -----+---------+-------+
   ````

### Keywords

    CREATE, MATERIALIZED, VIEW

### Best Practice
