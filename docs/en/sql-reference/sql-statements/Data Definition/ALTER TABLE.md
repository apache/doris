---
{
    "title": "ALTER TABLE",
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

# ALTER TABLE

## description

    This statement is used to modify an existing table. If no rollup index is specified, the base operation is the default.
    The statement is divided into three types of operations: schema change, rollup, partition
    These three types of operations cannot appear in an ALTER TABLE statement at the same time.
    Where schema change and rollup are asynchronous operations and are returned if the task commits successfully. You can then use the SHOW ALTER command to view the progress.
    Partition is a synchronous operation, and a command return indicates that execution is complete.

    grammar:
        ALTER TABLE [database.]table
        Alter_clause1[, alter_clause2, ...];

    The alter_clause is divided into partition, rollup, schema change, rename and bimmap index.

    Partition supports the following modifications
    Increase the partition
        grammar:
            ADD PARTITION [IF NOT EXISTS] partition_name
            Partition_desc ["key"="value"]
            [DISTRIBUTED BY HASH (k1[,k2 ...]) [BUCKETS num]]
        note:
            1) partition_desc supports two ways of writing:
                * VALUES LESS THAN [MAXVALUE|("value1", ...)]
                * VALUES [("value1", ...), ("value1", ...))
            1) The partition is the left closed right open interval. If the user only specifies the right boundary, the system will automatically determine the left boundary.
            2) If the bucket mode is not specified, the bucket method used by the built-in table is automatically used.
            3) If the bucket mode is specified, only the bucket number can be modified, and the bucket mode or bucket column cannot be modified.
            4) ["key"="value"] section can set some properties of the partition, see CREATE TABLE for details.
            5) Adding partitions to non-partitioned table is not supported.           

    2. Delete the partition
        grammar:
            DROP PARTITION [IF EXISTS] partition_name
        note:
            1) Use a partitioned table to keep at least one partition.
            2) Execute DROP PARTITION For a period of time, the deleted partition can be recovered by the RECOVER statement. See the RECOVER statement for details.
            3) If DROP PARTITION FORCE is executed, the system will not check whether the partition has unfinished transactions, the partition will be deleted directly and cannot be recovered, generally this operation is not recommended
 
    3. Modify the partition properties
        grammar:
            MODIFY PARTITION p1|(p1[, p2, ...]) SET ("key" = "value", ...)
        Description:
            1) The following attributes of the modified partition are currently supported.
                - storage_medium
                - storage_cooldown_time
                - replication_num 
                — in_memory
            2) For single-partition tables, partition_name is the same as the table name.
        
    Rollup supports the following ways to create:
    1. Create a rollup index
        grammar:
            ADD ROLLUP rollup_name (column_name1, column_name2, ...)
            [FROM from_index_name]
            [PROPERTIES ("key"="value", ...)]
 
            properties: Support setting timeout time, the default timeout time is 1 day.
        example:
            ADD ROLLUP r1(col1,col2) from r0
    1.2 Batch create rollup index
        grammar:
            ADD ROLLUP [rollup_name (column_name1, column_name2, ...)
                                    [FROM from_index_name]
                                    [PROPERTIES ("key"="value", ...)],...]
        example:
            ADD ROLLUP r1(col1,col2) from r0, r2(col3,col4) from r0
    1.3 note:
            1) If from_index_name is not specified, it is created by default from base index
            2) The columns in the rollup table must be existing columns in from_index
            3) In properties, you can specify the storage format. See CREATE TABLE for details.
            
    2. Delete the rollup index
        grammar:
            DROP ROLLUP rollup_name
            [PROPERTIES ("key"="value", ...)]
        example:
           DROP ROLLUP r1
    2.1 Batch Delete rollup index
        grammar: DROP ROLLUP [rollup_name [PROPERTIES ("key"="value", ...)],...]
        example: DROP ROLLUP r1,r2
    2.2 note:
            1) Cannot delete base index
               
            
    Schema change supports the following modifications:
    1. Add a column to the specified location of the specified index
        grammar:
            ADD COLUMN column_name column_type [KEY | agg_type] [DEFAULT "default_value"]
            [AFTER column_name|FIRST]
            [TO rollup_index_name]
            [PROPERTIES ("key"="value", ...)]
        note:
            1) Aggregate model If you add a value column, you need to specify agg_type
            2) Non-aggregate models (such as DUPLICATE KEY) If you add a key column, you need to specify the KEY keyword.
            3) You cannot add a column that already exists in the base index to the rollup index
                Recreate a rollup index if needed
            
    2. Add multiple columns to the specified index
        grammar:
            ADD COLUMN (column_name1 column_type [KEY | agg_type] DEFAULT "default_value", ...)
            [TO rollup_index_name]
            [PROPERTIES ("key"="value", ...)]
        note:
            1) Aggregate model If you add a value column, you need to specify agg_type
            2) Non-aggregate model If you add a key column, you need to specify the KEY keyword.
            3) You cannot add a column that already exists in the base index to the rollup index
            (You can recreate a rollup index if needed)
    
    3. Remove a column from the specified index
        grammar:
            DROP COLUMN column_name
            [FROM rollup_index_name]
        note:
            1) Cannot delete partition column
            2) If the column is removed from the base index, it will also be deleted if the column is included in the rollup index
        
    4. Modify the column type and column position of the specified index
        grammar:
            MODIFY COLUMN column_name column_type [KEY | agg_type] [NULL | NOT NULL] [DEFAULT "default_value"]
            [AFTER column_name|FIRST]
            [FROM rollup_index_name]
            [PROPERTIES ("key"="value", ...)]
        note:
            1) Aggregate model If you modify the value column, you need to specify agg_type
            2) Non-aggregate type If you modify the key column, you need to specify the KEY keyword.
            3) Only the type of the column can be modified. The other attributes of the column remain as they are (ie other attributes need to be explicitly written in the statement according to the original attribute, see example 8)
            4) The partition column cannot be modified
            5) The following types of conversions are currently supported (accuracy loss is guaranteed by the user)
                TINYINT/SMALLINT/INT/BIGINT/LARGEINT/FLOAT/DOUBLE convert to a wider range of numeric types
                TINTINT/SMALLINT/INT/BIGINT/LARGEINT/FLOAT/DOUBLE/DECIMAL is converted to VARCHAR
                VARCHAR supports modification of maximum length
                Convert VARCHAR/CHAR to TINYINT/SMALLINT/INT/BIGINT/LARGEINT/FLOAT/DOUBLE.
                Convert VARCHAR/CHAR to DATE (currently support six formats: "%Y-%m-%d", "%y-%m-%d", "%Y%m%d", "%y%m%d", "%Y/%m/%d, "%y/%m/%d")
                Convert DATETIME to DATE(Only year-month-day information is retained, For example: `2019-12-09 21:47:05` <--> `2019-12-09`)
                Convert DATE to DATETIME(Set hour, minute, second to zero, For example: `2019-12-09` <--> `2019-12-09 00:00:00`)
                Convert FLOAT to DOUBLE
                Convert INT to DATE (If the INT data fails to convert, the original data remains the same)
            6) Does not support changing from NULL to NOT NULL
                
    5. Reorder the columns of the specified index
        grammar:
            ORDER BY (column_name1, column_name2, ...)
            [FROM rollup_index_name]
            [PROPERTIES ("key"="value", ...)]
        note:
            1) All columns in index must be written
            2) value is listed after the key column
            
    6. Modify the properties of the table, currently supports modifying the bloom filter column, the colocate_with attribute and the dynamic_partition attribute,  the replication_num and default.replication_num.
        grammar:
            PROPERTIES ("key"="value")
        note:
            Can also be merged into the above schema change operation to modify, see the example below
    
    7. Enable batch delete support
        grammar:
            ENABLE FEATURE "BATCH_DELETE"
        note:
            1) Only support unique tables
            2) Batch deletion is supported for old tables, while new tables are already supported when they are created

    8. Enable the ability to import in order by the value of the sequence column
        grammer:
            ENABLE FEATURE "SEQUENCE_LOAD" WITH PROPERTIES ("function_column.sequence_type" = "Date")
        note:
            1) Only support unique tables
            2) The sequence_type is used to specify the type of the sequence column, which can be integral and time type
            3) Only the orderliness of newly imported data is supported. Historical data cannot be changed
     
    9. Modify default buckets number of partition 
        grammer:
            MODIFY DISTRIBUTION DISTRIBUTED BY HASH (k1[,k2 ...]) BUCKETS num
        note：
            1）Only support non colocate table with RANGE partition and HASH distribution

    10. Modify table comment
        grammer:
            MODIFY COMMENT "new table comment"

    11. Modify column comment
        grammer:
            MODIFY COLUMN col1 COMMENT "new column comment"

	12. Modify engine type

        Only the MySQL type can be changed to the ODBC type. The value of driver is the name of the driver in the odbc.init configuration.

        grammar:
            MODIFY ENGINE TO odbc PROPERTIES("driver" = "MySQL");
     
    Rename supports modification of the following names:
    1. Modify the table name
        grammar:
            RENAME new_table_name;
            
    2. Modify the rollup index name
        grammar:
            RENAME ROLLUP old_rollup_name new_rollup_name;
            
    3. Modify the partition name
        grammar:
            RENAME PARTITION old_partition_name new_partition_name;
  
    Bitmap index supports the following modifications:
    1. create bitmap index
        grammar:
            ADD INDEX [IF NOT EXISTS] index_name (column [, ...],) [USING BITMAP] [COMMENT 'balabala'];
        note:
            1. only supports bitmap index for current version
            2. BITMAP index only supports apply on single column
    2. drop index
        grammar:
            DROP INDEX [IF EXISTS] index_name;

## example

    [table]
    1. Modify the default number of replications of the table, which is used as default number of replications while creating new partition.
        ALTER TABLE example_db.my_table 
        SET ("default.replication_num" = "2");
        
    2. Modify the actual number of replications of a unpartitioned table (unpartitioned table only)
        ALTER TABLE example_db.my_table
        SET ("replication_num" = "3");

    [partition]
    1. Add partition, existing partition [MIN, 2013-01-01), add partition [2013-01-01, 2014-01-01), use default bucket mode
        ALTER TABLE example_db.my_table
        ADD PARTITION p1 VALUES LESS THAN ("2014-01-01");

    2. Increase the partition and use the new number of buckets
        ALTER TABLE example_db.my_table
        ADD PARTITION p1 VALUES LESS THAN ("2015-01-01")
        DISTRIBUTED BY HASH(k1) BUCKETS 20;

    3. Increase the partition and use the new number of copies
        ALTER TABLE example_db.my_table
        ADD PARTITION p1 VALUES LESS THAN ("2015-01-01")
        ("replication_num"="1");

    4. Modify the number of partition copies
        ALTER TABLE example_db.my_table
        MODIFY PARTITION p1 SET("replication_num"="1");
        
    5. Batch modify the specified partitions
        ALTER TABLE example_db.my_table
        MODIFY PARTITION (p1, p2, p4) SET("in_memory"="true");
        
    6. Batch modify all partitions
        ALTER TABLE example_db.my_table
        MODIFY PARTITION (*) SET("storage_medium"="HDD");

    7. Delete the partition
        ALTER TABLE example_db.my_table
        DROP PARTITION p1;
        
    8. Add a partition that specifies the upper and lower bounds

        ALTER TABLE example_db.my_table
        ADD PARTITION p1 VALUES [("2014-01-01"), ("2014-02-01"));

    [rollup]
    1. Create index: example_rollup_index, based on base index(k1,k2,k3,v1,v2). Columnar storage.
        ALTER TABLE example_db.my_table
        ADD ROLLUP example_rollup_index(k1, k3, v1, v2);
        
    2. Create index: example_rollup_index2, based on example_rollup_index(k1,k3,v1,v2)
        ALTER TABLE example_db.my_table
        ADD ROLLUP example_rollup_index2 (k1, v1)
        FROM example_rollup_index;

    3. Create index: example_rollup_index3, based on base index (k1, k2, k3, v1), custom rollup timeout time is one hour.
        
        ALTER TABLE example_db.my_table
        ADD ROLLUP example_rollup_index(k1, k3, v1)
        PROPERTIES("timeout" = "3600");
    
    3. Delete index: example_rollup_index2
        ALTER TABLE example_db.my_table
        DROP ROLLUP example_rollup_index2;

    [schema change]
    1. Add a key column new_col to the col1 of example_rollup_index (non-aggregate model)
        ALTER TABLE example_db.my_table
        ADD COLUMN new_col INT KEY DEFAULT "0" AFTER col1
        TO example_rollup_index;

    2. Add a value column new_col to the col1 of example_rollup_index (non-aggregate model)
          ALTER TABLE example_db.my_table
          ADD COLUMN new_col INT DEFAULT "0" AFTER col1
          TO example_rollup_index;

    3. Add a key column new_col (aggregation model) to col1 of example_rollup_index
          ALTER TABLE example_db.my_table
          ADD COLUMN new_col INT DEFAULT "0" AFTER col1
          TO example_rollup_index;

    4. Add a value column to the col1 of example_rollup_index. new_col SUM aggregation type (aggregation model)
          ALTER TABLE example_db.my_table
          ADD COLUMN new_col INT SUM DEFAULT "0" AFTER col1
          TO example_rollup_index;
   
    5. Add multiple columns to the example_rollup_index (aggregate model)
        ALTER TABLE example_db.my_table
        ADD COLUMN (col1 INT DEFAULT "1", col2 FLOAT SUM DEFAULT "2.3")
        TO example_rollup_index;
    
    6. Remove a column from example_rollup_index
        ALTER TABLE example_db.my_table
        DROP COLUMN col2
        FROM example_rollup_index;
        
    7. Modify the base index's col1 key column to be of type BIGINT and move to the col2 column
       (*Attention: Whether to modify the key column or the value column, complete column information need to be declared. For example, MODIFY COLUMN xxx COLUMNTYPE [KEY|agg_type]*)
        ALTER TABLE example_db.my_table
        MODIFY COLUMN col1 BIGINT KEY DEFAULT "1" AFTER col2;

    8. Modify the maximum length of the val1 column of the base index. The original val1 is (val1 VARCHAR(32) REPLACE DEFAULT "abc")
        ALTER TABLE example_db.my_table
        MODIFY COLUMN val1 VARCHAR(64) REPLACE DEFAULT "abc";
    
    9. Reorder the columns in example_rollup_index (set the original column order: k1, k2, k3, v1, v2)
        ALTER TABLE example_db.my_table
        ORDER BY (k3, k1, k2, v2, v1)
        FROM example_rollup_index;
        
    10. Perform both operations simultaneously
        ALTER TABLE example_db.my_table
        ADD COLUMN v2 INT MAX DEFAULT "0" AFTER k2 TO example_rollup_index,
        ORDER BY (k3,k1,k2,v2,v1) FROM example_rollup_index;

    11. Modify the bloom filter column of the table
        ALTER TABLE example_db.my_table SET ("bloom_filter_columns"="k1,k2,k3");

        Can also be merged into the above schema change operation (note that the syntax of multiple clauses is slightly different)
        ALTER TABLE example_db.my_table
        DROP COLUMN col2
        PROPERTIES ("bloom_filter_columns"="k1,k2,k3");

    12. Modify the Colocate property of the table

        ALTER TABLE example_db.my_table set ("colocate_with" = "t1");

    13. Change the bucketing mode of the table from Hash Distribution to Random Distribution

        ALTER TABLE example_db.my_table set ("distribution_type" = "random");
    
    14. Modify the dynamic partition properties of the table (support adding dynamic partition properties to tables without dynamic partition properties)
    
        ALTER TABLE example_db.my_table set ("dynamic_partition.enable" = "false");
    
        If you need to add dynamic partition attributes to a table without dynamic partition attributes, you need to specify all dynamic partition attributes.
        (Note:Adding dynamic partition attributes to non-partitioned table is not supported)
    
        ALTER TABLE example_db.my_table set ("dynamic_partition.enable"= "true", "dynamic_partition.time_unit" = "DAY", "dynamic_partition.end "= "3", "dynamic_partition.prefix" = "p", "dynamic_partition.buckets" = "32");
       

    15. Modify the in_memory property of the table

        ALTER TABLE example_db.my_table set ("in_memory" = "true");
    16. Enable batch delete support

        ALTER TABLE example_db.my_table ENABLE FEATURE "BATCH_DELETE"
    17. Enable the ability to import in order by the value of the Sequence column

        ALTER TABLE example_db.my_table ENABLE FEATURE "SEQUENCE_LOAD" WITH PROPERTIES ("function_column.sequence_type" = "Date")

    18. Modify the default buckets number of example_db.my_table to 50

        ALTER TABLE example_db.my_table MODIFY DISTRIBUTION DISTRIBUTED BY HASH(k1) BUCKETS 50;

    19. Modify table comment

        ALTER TABLE example_db.my_table MODIFY COMMENT "new comment";

    20. Modify column comment

        ALTER TABLE example_db.my_table MODIFY COLUMN k1 COMMENT "k1", MODIFY COLUMN k2 COMMENT "k2";

    21. Modify engine Type

        ALTER TABLE example_db.mysql_table MODIFY ENGINE TO odbc PROPERTIES("driver" = "MySQL");
        
    [rename]
    1. Modify the table named table1 to table2
        ALTER TABLE table1 RENAME table2;
        
    2. Modify the rollup index named rollup1 in the table example_table to rollup2
        ALTER TABLE example_table RENAME ROLLUP rollup1 rollup2;
        
    3. Modify the partition named p1 in the table example_table to p2
        ALTER TABLE example_table RENAME PARTITION p1 p2;
    
    [index]
    1. create index on table1 column siteid using bitmap 
        ALTER TABLE table1 ADD INDEX [IF NOT EXISTS] index_name  [USING BITMAP] (siteid) COMMENT 'balabala';
    2. drop bitmap index of table1
        ALTER TABLE table1 DROP INDEX [IF EXISTS] index_name;

## keyword

    ALTER, TABLE, ROLLUP, COLUMN, PARTITION, RENAME
