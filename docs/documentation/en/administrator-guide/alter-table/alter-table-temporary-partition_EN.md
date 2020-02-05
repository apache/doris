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

# Temporary Partition

In version 0.12, Doris supports Temporary Partition. A temporary partition is a copy of the **existing table partition** in Doris. This copy operation only copies the metadata of the partition, such as the range of partition values, the number of buckets, the replication number, etc., and **DOES NOT copy the data**.

Temporary partition is mainly used for overwrite operations on partitioned data. The user can first create a temporary partition corresponding to the table partition, then load the data into the temporary partition, and finally replace the table partition with the temporary partition to complete an atomic overwrite operation.

## Supported operations

The temporary partition supports add, delete, and replace operations.

### Add

You can add temporary partitions to a table with the `ALTER TABLE ADD TEMP PARTITION` statement:

```
ALTER TABLE tbl1 ADD TEMP PARTITION (p1, p2, p3);
```

For non-partitioned tables, you can also add "temporary partition", **The name of the temporary partition is the table name**:

```
ALTER TABLE tbl2 ADD TEMP PARTITION (tbl2);
```

See `HELP ALTER TABLE;` for more help and examples.

Some instructions for adding operations:

* The temporary partition is a copy of the metadata of the existing partition, so the partition corresponding to the temporary partition must already exist, and the temporary partition and the corresponding table partition will use the same partition name.
* There is a one-to-one correspondence between temporary partitions and table partitions. The temporary partition must have a corresponding table partition with the same name as the table partition. The table partition can have no corresponding temporary partition.

### Delete

A table's temporary partition can be dropped with the `ALTER TABLE DROP TEMP PARTITION` statement:

```
ALTER TABLE tbl1 DROP TEMP PARTITION (p1, p2, p3);
```

For non-partitioned tables, the name of the temporary partition is the table name:

```
ALTER TABLE tbl2 DROP TEMP PARTITION (tbl2);
```

See `HELP ALTER TABLE;` for more help and examples.

Some instructions for the delete operation:

* Deleting a non-existent temporary partition will not report an error.
* Deleting the temporary partition does not affect the data of the table partition.

### Replace

You can use the `ALTER TABLE REPLACE TEMP PARTITION` statement to replace table partitions with temporary partitions of a table.

```
ALTER TABLE tbl1 REPLACE TEMP PARTITION (p1, p2, p3);
```

For non-partitioned tables, the name of the temporary partition is the table name:

```
ALTER TABLE tbl2 REPLACE TEMP PARTITION (tbl2);
```

See `HELP ALTER TABLE;` for more help and examples.

Some instructions for the replacement operation:

* Replace operation is to replace the existing table partition with the corresponding temporary partition. That is, the temporary partition becomes a table partition, and the current table partition will be directly deleted, **and CANNOT be recovered**.

## Relationship to other operations

### DROP

* After using the Drop operation to directly drop the database or table, you can use the Recover command to recover the database or table (within a limited time). At the same time, the corresponding temporary partition will also be recovered together.
* After using the Alter command to drop a table partition, the corresponding temporary partition will also be dropped. The Recover command can only recover table partitions, and the corresponding temporary partitions cannot be recovered.

### TRUNCATE

* After using the Truncate command to empty the data of a table or partition, the corresponding temporary partition will be dropped and cannot be recovered.

### ALTER

* When the table has temporary partitions, you cannot use the Alter command to perform Schema Change, Rollup, and other changes on the table.
* When the table is undergoing an Alter operation, the temporary partition of the table cannot be operated.