# ALTER TABLE
## Description
This statement is used to modify an existing table. If no rollup index is specified, the default operation is base index.
该语句分为三种操作类型： schema change 、rollup 、partition
These three types of operations cannot appear in an ALTER TABLE statement at the same time.
Where schema change and rollup are asynchronous operations, task submission returns if it succeeds. You can then use the SHOW ALTER command to view progress.
Partition is a synchronous operation, and the return of the command indicates that the execution is complete.

Grammar:
ALTER TABLE [database.]table
alter_clause1[, alter_clause2, ...];

alter_clause 分为 partition 、rollup、schema change 和 rename 四种。

partition 支持如下几种修改方式
1. Adding partitions
Grammar:
ADD PARTITION [IF NOT EXISTS] partition_name VALUES LESS THAN [MAXVALUE|("value1")] ["key"="value"]
[DISTRIBUTED BY HASH (k1[,k2 ...]) [BUCKETS num]]
Be careful:
1) The partition is left-closed and right-open, the user specifies the right boundary, and the system automatically determines the left boundary.
2) If no bucket-dividing method is specified, the bucket-dividing method used in table-building will be used automatically.
3) If the barrel-dividing method is specified, only the number of barrels can be modified, but not the barrel-dividing method or the barrel-dividing column.
4) The ["key"= "value"] section can set some properties of the partition, as specified in CREATE TABLE

2. Delete partitions
Grammar:
DROP PARTITION [IF EXISTS] partition_name
Be careful:
1) A partitioned table should have at least one partition.
2) During the execution of DROP PARTITION, deleted partitions can be restored through RECOVER statements. See RECOVER statement for details

3. Modify partition attributes
Grammar:
MODIFY PARTITION partition u name SET ("key" ="value",...)
Explain:
1) Currently, three attributes, storage_medium, storage_cooldown_time and replication_num, are supported to modify partitions.
2) For a single partition table, partition_name is the same table name.

Rollup supports the following ways of creation:
One. 1.2.1.1.1.1.1.1.1.
Grammar:
ADD ROLLUP rollup_name (column_name1, column_name2, ...)
[FROM from index name]
[PROPERTIES ("key"="value", ...)]
Be careful:
1) If no from_index_name is specified, it is created by default from base index
2) The column in the rollup table must be an existing column from_index
3) In properties, you can specify the storage format. See CREATE TABLE for details.

2. 1.2.2.2.2.2.2.2.2.
Grammar:
DROP ROLLUP rollup_name
[PROPERTIES ("key"="value", ...)]
Be careful:
1) Base index cannot be deleted
2) During the execution of DROP ROLLUP, the deleted rollup index can be restored by RECOVER statement. See RECOVER statement for details


schema change 支持如下几种修改方式：
1. Add a column to the specified index location
Grammar:
ADD COLUMN column_name column_type [KEY | agg_type] [DEFAULT "default_value"]
[AFTER column_name|FIRST]
[TO rollup_index_name]
[PROPERTIES ("key"="value", ...)]
Be careful:
1) If the value column is added to the aggregation model, agg_type needs to be specified
2) If the key column is added to the non-aggregate model, KEY keywords need to be specified.
3) Cannot add columns already existing in base index in rollup index
If necessary, you can re-create a rollup index.

2. Add multiple columns to the specified index
Grammar:
ADD COLUMN (column_name1 column_type [KEY | agg_type] DEFAULT "default_value", ...)
[TO rollup_index_name]
[PROPERTIES ("key"="value", ...)]
Be careful:
1) If the value column is added to the aggregation model, agg_type needs to be specified
2) If the key column is added to the non-aggregate model, KEY keywords need to be specified.
3) Cannot add columns already existing in base index in rollup index
(You can re-create a rollup index if you need to)

3. Delete a column from the specified index
Grammar:
DROP COLUMN column_name
[FROM rollup_index_name]
Be careful:
1) Partition columns cannot be deleted
2) If a column is deleted from base index, it will also be deleted if it is included in rollup index

4. Modify the column type and column location of the specified index
Grammar:
MODIFY COLUMN column_name column_type [KEY | agg_type] [NULL | NOT NULL] [DEFAULT "default_value"]
[AFTER column_name|FIRST]
[FROM rollup_index_name]
[PROPERTIES ("key"="value", ...)]
Be careful:
1) The aggregation model needs to specify agg_type if it modifies the value column
2) If you modify the key column for a non-aggregated type, you need to specify the KEY keyword
3) The type of column can only be modified, and the other attributes of the column remain the same (that is, other attributes should be explicitly written in the statement according to the original attributes, see example 8).
4) Partition column cannot be modified
5) The following types of conversion are currently supported (accuracy loss is guaranteed by users)
TINYINT/SMALLINT/INT/BIGINT is converted to TINYINT/SMALLINT/INT/BIGINT/DOUBLE.
LARGEINT 转换成 DOUBLE
VARCHAR 25345;'20462;' 25913;'38271;' 24230s;
6) Conversion from NULL to NOT NULL is not supported

5. Reordering columns with specified index
Grammar:
ORDER BY (column_name1, column_name2, ...)
[FROM rollup_index_name]
[PROPERTIES ("key"="value", ...)]
Be careful:
1) All columns in index should be written out
2) Value is listed after the key column

6. Modify table attributes, which currently support modifying bloom filter columns and colocate_with attributes
Grammar:
PROPERTIES ("key"="value")
Be careful:
You can also incorporate it into the schema change operation above to modify it, as shown in the following example


Rename supports the modification of the following names:
1. Modify the table name
Grammar:
RENAME new_table_name;

2. 1.2.2.5.5.5.5.;5.5.5.5.5.5.
Grammar:
RENAME ROLLUP old_rollup_name new_rollup_name;

3. 修改 partition 名称
Grammar:
Rename old partition name and new partition name

## example
[partition]
1. Increase partitions, existing partitions [MIN, 2013-01-01], increase partitions [2013-01-01, 2014-01-01], using default bucket partitioning
ALTER TABLE example_db.my_table
ADD PARTITION p1 VALUES LESS THAN ("2014-01-01");

2. Increase partitions and use new buckets
ALTER TABLE example_db.my_table
ADD PARTITION p1 VALUES LESS THAN ("2015-01-01")
DISTRIBUTED BY HASH(k1) BUCKETS 20;

3. Delete partitions
ALTER TABLE example_db.my_table
DROP PARTITION p1;

[rollup]
1. Create index: example_rollup_index, based on base index (k1, k2, k3, v1, v2). Formula storage.
ALTER TABLE example_db.my_table
ADD ROLLUP example_rollup_index(k1, k3, v1, v2)
PROPERTIES("storage_type"="column");

2. Create index: example_rollup_index2, based on example_rollup_index (k1, k3, v1, v2)
ALTER TABLE example_db.my_table
ADD ROLLUP example_rollup_index2 (k1, v1)
FROM example_rollup_index;

3. Delete index: example_rollup_index2
ALTER TABLE example_db.my_table
DROP ROLLUP example_rollup_index2;

[schema change]
1. Add a key column new_col (non-aggregate model) to col1 of example_rollup_index
ALTER TABLE example_db.my_table
ADD COLUMN new_col INT KEY DEFAULT "0" AFTER col1
TO example_rollup_index;

2. Add a value column new_col (non-aggregate model) to col1 of example_rollup_index
ALTER TABLE example_db.my_table
ADD COLUMN new_col INT DEFAULT "0" AFTER col1
TO example_rollup_index;

3. Add a key column new_col (aggregation model) to col1 of example_rollup_index
ALTER TABLE example_db.my_table
ADD COLUMN new_col INT DEFAULT "0" AFTER col1
TO example_rollup_index;

4. Add a value column new_col SUM aggregation type (aggregation model) to col1 of example_rollup_index
ALTER TABLE example_db.my_table
ADD COLUMN new_col INT SUM DEFAULT "0" AFTER col1
TO example_rollup_index;

5. Add multiple columns to example_rollup_index (aggregation model)
ALTER TABLE example_db.my_table
ADD COLUMN (col1 INT DEFAULT "1", col2 FLOAT SUM DEFAULT "2.3")
TO example_rollup_index;

6. Delete a column from example_rollup_index
ALTER TABLE example_db.my_table
DROP COLUMN col2
FROM example_rollup_index;

7. Modify the col1 column type of base index to BIGINT and move to the back of col2 column
ALTER TABLE example_db.my_table
MODIFY COLUMN col1 BIGINT DEFAULT "1" AFTER col2;

8. 修改 base index 的 val1 列最大长度。原 val1 为 (val1 VARCHAR(32) REPLACE DEFAULT "abc")
ALTER TABLE example_db.my_table
MODIFY COLUMN val1 VARCHAR(64) REPLACE DEFAULT "abc";

9. Rearrange the columns in example_rollup_index (set the original column order to k1, k2, k3, v1, v2)
ALTER TABLE example_db.my_table
ORDER BY (k3,k1,k2,v2,v1)
FROM example_rollup_index;

10. Perform two operations simultaneously
ALTER TABLE example_db.my_table
ADD COLUMN v2 INT MAX DEFAULT "0" AFTER k2 TO example_rollup_index,
ORDER BY (k3,k1,k2,v2,v1) FROM example_rollup_index;

11. 20462;- 259130;-bloom filter -210151;
ALTER TABLE example_db.my_table SET ("bloom_filter_columns"="k1,k2,k3");

You can also incorporate it into the schema change operation above (note that the grammar of multiple clauses is slightly different)
ALTER TABLE example_db.my_table
DROP COLUMN col2
PROPERTIES ("bloom_filter_columns"="k1,k2,k3");

12. Modify the Colocate property of the table
ALTER TABLE example_db.my_table set ("colocate_with"="t1");

13. Change the Distribution type from Random to Hash

ALTER TABLE example_db.my_table set ("distribution_type" = "hash");

[Rename]
1. Modify the table named Table 1 to table2
ALTER TABLE table1 RENAME table2;

2. 将表 example_table 中名为 rollup1 的 rollup index 修改为 rollup2
ALTER TABLE example_table RENAME ROLLUP rollup1 rollup2;

3. 将表 example_table 中名为 p1 的 partition 修改为 p2
ALTER TABLE example_table RENAME PARTITION p1 p2;

## keyword
ALTER,TABLE,ROLLUP,COLUMN,PARTITION,RENAME

