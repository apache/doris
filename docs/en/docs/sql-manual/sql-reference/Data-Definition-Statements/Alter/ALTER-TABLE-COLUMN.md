---
{
    "title": "ALTER-TABLE-COLUMN",
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

## Name

ALTER TABLE COLUMN

### Description

This statement is used to perform a schema change operation on an existing table. The schema change is asynchronous, and the task is returned when the task is submitted successfully. After that, you can use the [SHOW ALTER TABLE COLUMN](../../Show-Statements/SHOW-ALTER.md) command to view the progress.

Doris has the concept of materialized index after table construction. After successful table construction, it is the base table and the materialized index is the base index. rollup index can be created based on the base table. Both base index and rollup index are materialized indexes. If rollup_index_name is not specified during the schema change operation, the operation is based on the base table by default.

:::tip
Doris 1.2.0 supports light schema change for light scale structure changes, and addition and subtraction operations for value columns can be completed more quickly and synchronously. You can manually specify "light_schema_change" = 'true' when creating a table. This parameter is enabled by default for versions 2.0.0 and later.
:::

### Grammar:

```sql
ALTER TABLE [database.]table alter_clause;
```

The alter_clause of schema change supports the following modification methods:

**1. Add a column to the specified position at the specified index**

**Grammar**

```sql
ALTER TABLE [database.]table table_name ADD COLUMN column_name column_type [KEY | agg_type] [DEFAULT "default_value"]
[AFTER column_name|FIRST]
[TO rollup_index_name]
[PROPERTIES ("key"="value", ...)]
```
  
**Example**
  
1. Add a key column new_col to example_db.my_table after key_1 (non-aggregated model)
  
  ```sql
  ALTER TABLE example_db.my_table
  ADD COLUMN new_col INT KEY DEFAULT "0" AFTER key_1;
  ```
  
2. Add a value column new_col to example_db.my_table after value_1 (non-aggregate model)
  
  ```sql
  ALTER TABLE example_db.my_table
  ADD COLUMN new_col INT DEFAULT "0" AFTER value_1;
  ```

3. Add a key column new_col (aggregate model) to example_db.my_table after key_1

  ```sql
  ALTER TABLE example_db.my_table
  ADD COLUMN new_col INT KEY DEFAULT "0" AFTER key_1;
  ```

4. Add a value column to example_db.my_table after value_1 new_col SUM Aggregation type (aggregation model)

  ```sql
  ALTER TABLE example_db.my_table   
  ADD COLUMN new_col INT SUM DEFAULT "0" AFTER value_1; 
  ```

5. Add new_col to the first column position of the example_db.my_table table (non-aggregated model)

  ```sql
  ALTER TABLE example_db.my_table
  ADD COLUMN new_col INT KEY DEFAULT "0" FIRST;
  ```

:::tip 
- If you add a value column to the aggregation model, you need to specify agg_type
- For non-aggregated models (such as DUPLICATE KEY), if you add a key column, you need to specify the KEY keyword
- You cannot add columns that already exist in the base index to the rollup index (you can recreate a rollup index if necessary)
:::


**2. Add multiple columns to the specified index**

**Grammar**
  
```sql
ALTER TABLE [database.]table table_name ADD COLUMN (column_name1 column_type [KEY | agg_type] DEFAULT "default_value", ...)
[TO rollup_index_name]
[PROPERTIES ("key"="value", ...)]
```

  
**Example**
  
1. Add multiple columns to example_db.my_table, where new_col and new_col2 are SUM aggregate types (aggregate model)
  
  ```sql
  ALTER TABLE example_db.my_table
  ADD COLUMN (new_col1 INT SUM DEFAULT "0" ,new_col2 INT SUM DEFAULT "0");
  ```
  
2. Add multiple columns to example_db.my_table (non-aggregated model), where new_col1 is the KEY column and new_col2 is the value column
  
  ```sql
  ALTER TABLE example_db.my_table
  ADD COLUMN (new_col1 INT key DEFAULT "0" , new_col2 INT DEFAULT "0");
  ```

:::tip
  - If you add a value column to the aggregation model, you need to specify agg_type
  - If you add a key column to the aggregation model, you need to specify the KEY keyword
  - You cannot add columns that already exist in the base index to the rollup index (you can recreate a rollup index if necessary) 
:::

**3. Delete a column from the specified index**

**Grammar***
  
  ```sql
  ALTER TABLE [database.]table table_name DROP COLUMN column_name
  [FROM rollup_index_name]
  ```

**Example**
   
1. Delete column col1 from example_db.my_table
  
  ```sql
  ALTER TABLE example_db.my_table DROP COLUMN col1;
  ```

:::tip
  - Cannot drop partition column
  - The aggregate model cannot delete KEY columns
  - If the column is removed from the base index, it will also be removed if it is included in the rollup index 
:::

**4. Modify the column type and column position of the specified index**

**Grammar**
  
```sql
ALTER TABLE [database.]table table_name MODIFY COLUMN column_name column_type [KEY | agg_type] [NULL | NOT NULL] [DEFAULT "default_value"]
[AFTER column_name|FIRST]
[FROM rollup_index_name]
[PROPERTIES ("key"="value", ...)]
```
  
**Example**
  
1. Modify the type of the key column col1 of the base index to BIGINT and move it to the back of the col2 column

  ```sql
  ALTER TABLE example_db.my_table 
  MODIFY COLUMN col1 BIGINT KEY DEFAULT "1" AFTER col2;
  ```

  :::tip
  Whether you modify the key column or the value column, you need to declare complete column information
  :::

2. Modify the maximum length of the val1 column of base index. The original val1 is (val1 VARCHAR(32) REPLACE DEFAULT "abc")

  ```sql
  ALTER TABLE example_db.my_table 
  MODIFY COLUMN val1 VARCHAR(64) REPLACE DEFAULT "abc";
  ```

  :::tip
  You can only modify the column's data type; other attributes of the column must remain unchanged.
  :::

3. Modify the length of a field in the Key column of the Duplicate key table

  ```sql
  ALTER TABLE example_db.my_table 
  MODIFY COLUMN k3 VARCHAR(50) KEY NULL COMMENT 'to 50';
  ```

:::tip
  - If you modify the value column in the aggregation model, you need to specify agg_type
  - If you modify the key column for non-aggregate types, you need to specify the KEY keyword
  - Only the type of the column can be modified, and other attributes of the column remain as they are (that is, other attributes need to be explicitly written in the statement according to the   original attributes, see example 8)
  - Partitioning and bucketing columns cannot be modified in any way
  - The following types of conversions are currently supported (loss of precision is guaranteed by the user)
    - Conversion of TINYINT/SMALLINT/INT/BIGINT/LARGEINT/FLOAT/DOUBLE types to larger numeric types
    - Convert TINTINT/SMALLINT/INT/BIGINT/LARGEINT/FLOAT/DOUBLE/DECIMAL to VARCHAR
    - VARCHAR supports modifying the maximum length
    - VARCHAR/CHAR converted to TINTINT/SMALLINT/INT/BIGINT/LARGEINT/FLOAT/DOUBLE
    - Convert VARCHAR/CHAR to DATE (currently supports "%Y-%m-%d", "%y-%m-%d", "%Y%m%d", "%y%m%d", "%Y/%m/%d, "%y/%m/%d" six formats)
    - Convert DATETIME to DATE (only keep year-month-day information, for example: `2019-12-09 21:47:05` <--> `2019-12-09`)
    - DATE is converted to DATETIME (hours, minutes and seconds are automatically filled with zeros, for example: `2019-12-09` <--> `2019-12-09 00:00:00`)
    - Convert FLOAT to DOUBLE
    - INT is converted to DATE (if the INT type data is illegal, the conversion fails, and the original data remains unchanged)
    - All can be converted to STRING except DATE and DATETIME, but STRING cannot be converted to any other type 
:::

**5. Reorder the column at the specified index**

**Grammar**
  
  ```sql
  ALTER TABLE [database.]table table_name ORDER BY (column_name1, column_name2, ...)
  [FROM rollup_index_name]
  [PROPERTIES ("key"="value", ...)]
  ```
  
**Example**
  
1. Adjust the order of the key and value columns of example_db.my_table (non-aggregate model)
  
  ```sql
  CREATE TABLE `my_table`(
  `k_1` INT NULL,
  `k_2` INT NULL,
  `v_1` INT NULL,
  `v_2` varchar NULL,
  `v_3` varchar NULL
  ) ENGINE=OLAP
  DUPLICATE KEY(`k_1`, `k_2`)
  COMMENT 'OLAP'
  DISTRIBUTED BY HASH(`k_1`) BUCKETS 5
  PROPERTIES (
  "replication_allocation" = "tag.location.default: 1"
  );

  ALTER TABLE example_db.my_table ORDER BY (k_2,k_1,v_3,v_2,v_1);

  mysql> desc my_table;
  +-------+------------+------+-------+---------+-------+
  | Field | Type       | Null | Key   | Default | Extra |
  +-------+------------+------+-------+---------+-------+
  | k_2   | INT        | Yes  | true  | NULL    |       |
  | k_1   | INT        | Yes  | true  | NULL    |       |
  | v_3   | VARCHAR(*) | Yes  | false | NULL    | NONE  |
  | v_2   | VARCHAR(*) | Yes  | false | NULL    | NONE  |
  | v_1   | INT        | Yes  | false | NULL    | NONE  |
  +-------+------------+------+-------+---------+-------+
  ```
  
2. Do two actions simultaneously
  
  ```sql
  CREATE TABLE `my_table` (
  `k_1` INT NULL,
  `k_2` INT NULL,
  `v_1` INT NULL,
  `v_2` varchar NULL,
  `v_3` varchar NULL
  ) ENGINE=OLAP
  DUPLICATE KEY(`k_1`, `k_2`)
  COMMENT 'OLAP'
  DISTRIBUTED BY HASH(`k_1`) BUCKETS 5
  PROPERTIES (
  "replication_allocation" = "tag.location.default: 1"
  );

  ALTER TABLE example_db.my_table
  ADD COLUMN col INT DEFAULT "0" AFTER v_1,
  ORDER BY (k_2,k_1,v_3,v_2,v_1,col);

  mysql> desc my_table;
  +-------+------------+------+-------+---------+-------+
  | Field | Type       | Null | Key   | Default | Extra |
  +-------+------------+------+-------+---------+-------+
  | k_2   | INT        | Yes  | true  | NULL    |       |
  | k_1   | INT        | Yes  | true  | NULL    |       |
  | v_3   | VARCHAR(*) | Yes  | false | NULL    | NONE  |
  | v_2   | VARCHAR(*) | Yes  | false | NULL    | NONE  |
  | v_1   | INT        | Yes  | false | NULL    | NONE  |
  | col   | INT        | Yes  | false | 0       | NONE  |
  +-------+------------+------+-------+---------+-------+
  ```

:::tip
  - All columns in index are written out
  - the value column comes after the key column
  - You can adjust the key column only within the range of the key column. The same applies to the value column
:::
  
### Keywords

```text
ALTER, TABLE, COLUMN, ALTER TABLE
```

### Best Practice
