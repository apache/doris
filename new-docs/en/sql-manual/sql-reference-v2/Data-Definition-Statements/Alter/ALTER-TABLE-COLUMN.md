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

## ALTER-TABLE-COLUMN

### Name

ALTER TABLE  COLUMN

### Description

```text
This statement is used to perform a schema change operation on an existing table. The schema change is asynchronous, and the task is returned when the task is submitted successfully. After that, you can use the SHOW ALTER command to view the progress.

grammar:
    ALTER TABLE [database.]table alter_clause;

The alter_clause of schema change supports the following modification methods:
1. Add a column to the specified position at the specified index
    grammar:
        ADD COLUMN column_name column_type [KEY | agg_type] [DEFAULT "default_value"]
        [AFTER column_name|FIRST]
        [TO rollup_index_name]
        [PROPERTIES ("key"="value", ...)]
    Notice:
        1) If you add a value column to the aggregation model, you need to specify agg_type
        2) For non-aggregated models (such as DUPLICATE KEY), if you add a key column, you need to specify the KEY keyword
        3) The column that already exists in the base index cannot be added to the rollup index
            A rollup index can be recreated if needed)
        
2. Add multiple columns to the specified index
    grammar:
        ADD COLUMN (column_name1 column_type [KEY | agg_type] DEFAULT "default_value", ...)
        [TO rollup_index_name]
        [PROPERTIES ("key"="value", ...)]
    Notice:
        1) If you add a value column to the aggregation model, you need to specify agg_type
        2) If a non-aggregate model adds a key column, you need to specify the KEY keyword
        3) The column that already exists in the base index cannot be added to the rollup index
        (You can recreate a rollup index if needed)

3. Delete a column from the specified index
    grammar:
        DROP COLUMN column_name
        [FROM rollup_index_name]
    Notice:
        1) Cannot delete partition column
        2) If the column is deleted from the base index, if the column is included in the rollup index, it will also be deleted.

4. Modify the column type and column position of the specified index
    grammar:
        MODIFY COLUMN column_name column_type [KEY | agg_type] [NULL | NOT NULL] [DEFAULT "default_value"]
        [AFTER column_name|FIRST]
        [FROM rollup_index_name]
        [PROPERTIES ("key"="value", ...)]
    Notice:
        1) If you modify the value column in the aggregation model, you need to specify agg_type
        2) If you modify the key column for non-aggregate types, you need to specify the KEY keyword
        3) Only the type of the column can be modified, and other attributes of the column remain as they are (that is, other attributes need to be explicitly written in the statement according to the original attributes, see example 8)
        4) The partition column and bucket column cannot be modified in any way
        5) The following types of conversions are currently supported (loss of precision is guaranteed by the user)
            Conversion of TINYINT/SMALLINT/INT/BIGINT/LARGEINT/FLOAT/DOUBLE types to larger numeric types
            Convert TINTINT/SMALLINT/INT/BIGINT/LARGEINT/FLOAT/DOUBLE/DECIMAL to VARCHAR
            VARCHAR supports modifying the maximum length
            VARCHAR/CHAR to TINTINT/SMALLINT/INT/BIGINT/LARGEINT/FLOAT/DOUBLE
            Convert VARCHAR/CHAR to DATE (currently supports "%Y-%m-%d", "%y-%m-%d", "%Y%m%d", "%y%m%d", " %Y/%m/%d, "%y/%m/%d" six formats)
            Convert DATETIME to DATE (only keep year-month-day information, for example: `2019-12-09 21:47:05` <--> `2019-12-09`)
            DATE is converted to DATETIME (hours, minutes and seconds are automatically filled with zeros, for example: `2019-12-09` <--> `2019-12-09 00:00:00`)
            Convert FLOAT to DOUBLE
            Convert INT to DATE (if the INT type data is illegal, the conversion fails, and the original data remains unchanged)
        6) Converting from NULL to NOT NULL is not supported.
5. Reorder the column at the specified index
    grammar:
        ORDER BY (column_name1, column_name2, ...)
        [FROM rollup_index_name]
        [PROPERTIES ("key"="value", ...)]
    Notice:
        1) All columns in index must be written out
        2) The value column is after the key column
```

### Example

```text
1. Add a key column new_col after col1 of example_rollup_index (non-aggregated model)
    ALTER TABLE example_db.my_table
    ADD COLUMN new_col INT KEY DEFAULT "0" AFTER col1
    TO example_rollup_index;

2. Add a value column new_col after col1 of example_rollup_index (non-aggregation model)
      ALTER TABLE example_db.my_table
      ADD COLUMN new_col INT DEFAULT "0" AFTER col1
      TO example_rollup_index;

3. Add a key column new_col (aggregation model) after col1 of example_rollup_index
      ALTER TABLE example_db.my_table
      ADD COLUMN new_col INT DEFAULT "0" AFTER col1
      TO example_rollup_index;

4. Add a value column new_col SUM aggregation type (aggregation model) after col1 of example_rollup_index
      ALTER TABLE example_db.my_table
      ADD COLUMN new_col INT SUM DEFAULT "0" AFTER col1
      TO example_rollup_index;

5. Add multiple columns to example_rollup_index (aggregation model)
    ALTER TABLE example_db.my_table
    ADD COLUMN (col1 INT DEFAULT "1", col2 FLOAT SUM DEFAULT "2.3")
    TO example_rollup_index;

6. Remove a column from example_rollup_index
    ALTER TABLE example_db.my_table
    DROP COLUMN col2
    FROM example_rollup_index;
    
7. Modify the type of the key column col1 of the base index to BIGINT and move it to the back of the col2 column
   (*Note that you need to declare complete column information whether you modify the key column or the value column*) For example: MODIFY COLUMN xxx COLUMNTYPE [KEY|agg_type]
    ALTER TABLE example_db.my_table
    MODIFY COLUMN col1 BIGINT KEY DEFAULT "1" AFTER col2;

8. Modify the maximum length of the val1 column of base index. The original val1 is (val1 VARCHAR(32) REPLACE DEFAULT "abc")
    ALTER TABLE example_db.my_table
    MODIFY COLUMN val1 VARCHAR(64) REPLACE DEFAULT "abc";

9. Reorder the columns in example_rollup_index (set the original column order as: k1,k2,k3,v1,v2)
    ALTER TABLE example_db.my_table
    ORDER BY (k3,k1,k2,v2,v1)
    FROM example_rollup_index;
    
10. Do Two Actions Simultaneously
    ALTER TABLE example_db.my_table
    ADD COLUMN v2 INT MAX DEFAULT "0" AFTER k2 TO example_rollup_index,
    ORDER BY (k3,k1,k2,v2,v1) FROM example_rollup_index;
```

### Keywords

    ALTER, TABLE, COLUMN

### Best Practice

