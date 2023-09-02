---
{
    "title": "Batch Delete",
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

# Batch Delete
Currently, Doris supports multiple import methods such as [broker load](../import/import-way/broker-load-manual.md), [routine load](../import/import-way/routine-load-manual.md), [stream load](../import/import-way/stream-load-manual.md), etc. The data can only be deleted through the delete statement at present. When the delete statement is used to delete, a new data version will be generated every time delete is executed. Frequent deletion will seriously affect the query performance, and when using the delete method to delete, it is achieved by generating an empty rowset to record the deletion conditions. Each time you read, you must filter the deletion jump conditions. Also when there are many conditions, Performance affects. Compared with other systems, the implementation of greenplum is more like a traditional database product. Snowflake is implemented through the merge syntax.

For scenarios similar to the import of cdc data, insert and delete in the data data generally appear interspersed. In this scenario, our current import method is not enough, even if we can separate insert and delete, it can solve the import problem , But still cannot solve the problem of deletion. Use the batch delete function to solve the needs of these scenarios.
There are three ways to merge data import:
1. APPEND: All data are appended to existing data
2. DELETE: delete all rows with the same key column value as the imported data(When there is a [`sequence`](sequence-column-manual.md) column in the table, the same primary key and the logic of the size of the sequence column must be satisfied at the same time to delete it correctly, see use case 4 below for details.)
3. MERGE: APPEND or DELETE according to DELETE ON decision

## Fundamental
This is achieved by adding a hidden column `__DORIS_DELETE_SIGN__`, because we are only doing batch deletion on the unique model, so we only need to add a hidden column whose type is bool and the aggregate function is replace. In be, the various aggregation write processes are the same as normal columns, and there are two read schemes:

Remove `__DORIS_DELETE_SIGN__` when fe encounters extensions such as *, and add the condition of `__DORIS_DELETE_SIGN__ != true` by default When be reads, a column is added for judgment, and the condition is used to determine whether to delete.

### Import

When importing, set the value of the hidden column to the value of the `DELETE ON` expression during fe parsing. The other aggregation behaviors are the same as the replace aggregation column.

### Read

When reading, add the condition of `__DORIS_DELETE_SIGN__ != true` to all olapScanNodes with hidden columns, be does not perceive this process and executes normally.

### Cumulative Compaction

In Cumulative Compaction, hidden columns are treated as normal columns, and the compaction logic remains unchanged.

### Base Compaction

In Base Compaction, delete the rows marked for deletion to reduce the space occupied by data.

## Enable Bulk Delete Support

There are two ways of enabling batch delete support:

1. By adding `enable_batch_delete_by_default=true` in the fe configuration file, all newly created tables after restarting fe support batch deletion, this option defaults to false

2. For tables that have not changed the above fe configuration or for existing tables that do not support the bulk delete function, you can use the following statement:
   `ALTER TABLE tablename ENABLE FEATURE "BATCH_DELETE"` to enable the batch delete.

If you want to determine whether a table supports batch delete, you can set a session variable to display the hidden columns `SET show_hidden_columns=true`, and then use `desc tablename`, if there is a `__DORIS_DELETE_SIGN__` column in the output, it is supported, if not, it is not supported

### Syntax Description
The syntax design of the import is mainly to add a column mapping that specifies the field of the delete marker column, and it is necessary to add a column to the imported data. The syntax of various import methods is as follows:

#### Stream Load

The writing method of `Stream Load` adds a field to set the delete label column in the columns field in the header. Example
`-H "columns: k1, k2, label_c3" -H "merge_type: [MERGE|APPEND|DELETE]" -H "delete: label_c3=1"`

#### Broker Load

The writing method of `Broker Load` sets the field of the delete marker column at `PROPERTIES`, the syntax is as follows:

```sql
LOAD LABEL db1.label1
(
    [MERGE|APPEND|DELETE] DATA INFILE("hdfs://abc.com:8888/user/palo/test/ml/file1")
    INTO TABLE tbl1
    COLUMNS TERMINATED BY ","
    (tmp_c1,tmp_c2, label_c3)
    SET
    (
        id=tmp_c2,
        name=tmp_c1,
    )
    [DELETE ON label_c3=true]
)
WITH BROKER'broker'
(
    "username"="user",
    "password"="pass"
)
PROPERTIES
(
    "timeout" = "3600"
);

```

#### Routine Load

The writing method of `Routine Load` adds a mapping to the `columns` field. The mapping method is the same as above. The syntax is as follows:

```sql
   CREATE ROUTINE LOAD example_db.test1 ON example_tbl
    [WITH MERGE|APPEND|DELETE]
    COLUMNS(k1, k2, k3, v1, v2, label),
    WHERE k1> 100 and k2 like "%doris%"
    [DELETE ON label=true]
    PROPERTIES
    (
        "desired_concurrent_number"="3",
        "max_batch_interval" = "20",
        "max_batch_rows" = "300000",
        "max_batch_size" = "209715200",
        "strict_mode" = "false"
    )
    FROM KAFKA
    (
        "kafka_broker_list" = "broker1:9092,broker2:9092,broker3:9092",
        "kafka_topic" = "my_topic",
        "kafka_partitions" = "0,1,2,3",
        "kafka_offsets" = "101,0,0,200"
    );
```

## Note
1. Since import operations other than stream load may be executed out of order inside doris, if it is not stream load when importing using the `MERGE` method, it needs to be used with load sequence. For the specific syntax, please refer to the [sequence](sequence-column-manual.md) column related documents
2. `DELETE ON` condition can only be used with MERGE.
3. if session variable `SET show_hidden_columns = true` was executed before running import task to show whether table support batch delete feature, then execute `select count(*) from xxx` statement in the same session after finishing `DELETE/MERGE` import task, it will result in a unexpected result that the statement result set will include the deleted results. To avoid this problem you should execute `SET show_hidden_columns = false` before select statement or open a new session to run the select statement.

## Usage Examples

### Check if Bulk Delete Support is Enabled

```sql
mysql> SET show_hidden_columns=true;
Query OK, 0 rows affected (0.00 sec)

mysql> DESC test;
+-----------------------+--------------+------+-------+---------+---------+
| Field                 | Type         | Null | Key   | Default | Extra   |
+-----------------------+--------------+------+-------+---------+---------+
| name                  | VARCHAR(100) | No   | true  | NULL    |         |
| gender                | VARCHAR(10)  | Yes  | false | NULL    | REPLACE |
| age                   | INT          | Yes  | false | NULL    | REPLACE |
| __DORIS_DELETE_SIGN__ | TINYINT      | No   | false | 0       | REPLACE |
+-----------------------+--------------+------+-------+---------+---------+
4 rows in set (0.00 sec)
```

### Stream Load Usage Examples

1. Import data normally:
```shell
curl --location-trusted -u root: -H "column_separator:," -H "columns: siteid, citycode, username, pv" -H "merge_type: APPEND" -T ~/table1_data http://127.0.0.1: 8130/api/test/table1/_stream_load
```
The APPEND condition can be omitted, which has the same effect as the following statement:
```shell
curl --location-trusted -u root: -H "column_separator:," -H "columns: siteid, citycode, username, pv" -T ~/table1_data http://127.0.0.1:8130/api/test/table1 /_stream_load
```
2. Delete all data with the same key as the imported data
```shell
curl --location-trusted -u root: -H "column_separator:," -H "columns: siteid, citycode, username, pv" -H "merge_type: DELETE" -T ~/table1_data http://127.0.0.1: 8130/api/test/table1/_stream_load
```
Before load:
```sql
+--------+----------+----------+------+
| siteid | citycode | username | pv   |
+--------+----------+----------+------+
|      3 |        2 | tom      |    2 |
|      4 |        3 | bush     |    3 |
|      5 |        3 | helen    |    3 |
+--------+----------+----------+------+
```
Load data:
```
3,2,tom,0
```
After load:
```sql
+--------+----------+----------+------+
| siteid | citycode | username | pv   |
+--------+----------+----------+------+
|      4 |        3 | bush     |    3 |
|      5 |        3 | helen    |    3 |
+--------+----------+----------+------+
```
3. Import the same row as the key column of the row with `site_id=1`
```shell
curl --location-trusted -u root: -H "column_separator:," -H "columns: siteid, citycode, username, pv" -H "merge_type: MERGE" -H "delete: siteid=1" -T ~/ table1_data http://127.0.0.1:8130/api/test/table1/_stream_load
```
Before load:
```sql
+--------+----------+----------+------+
| siteid | citycode | username | pv   |
+--------+----------+----------+------+
|      4 |        3 | bush     |    3 |
|      5 |        3 | helen    |    3 |
|      1 |        1 | jim      |    2 |
+--------+----------+----------+------+
```
Load data:
```
2,1,grace,2
3,2,tom,2
1,1,jim,2
```
After load:
```sql
+--------+----------+----------+------+
| siteid | citycode | username | pv   |
+--------+----------+----------+------+
|      4 |        3 | bush     |    3 |
|      2 |        1 | grace    |    2 |
|      3 |        2 | tom      |    2 |
|      5 |        3 | helen    |    3 |
+--------+----------+----------+------+
```

4. When the table has the sequence column, delete all data with the same key as the imported data

```bash
curl --location-trusted -u root: -H "column_separator:," -H "columns: name, gender, age" -H "function_column.sequence_col: age" -H "merge_type: DELETE"  -T ~/table1_data http://127.0.0.1:8130/api/test/table1/_stream_load
```

When the unique table has the sequence column, sequence column is used as the basis for the replacement order of the REPLACE aggregate function under the same key column, and the larger value can replace the smaller value.
If you want delete some data, the imported data must have the same key and the sequence column must be larger or equal than before.

for example, one table like this:
```sql
mysql> SET show_hidden_columns=true;
Query OK, 0 rows affected (0.00 sec)

mysql> DESC table1;
+------------------------+--------------+------+-------+---------+---------+
| Field                  | Type         | Null | Key   | Default | Extra   |
+------------------------+--------------+------+-------+---------+---------+
| name                   | VARCHAR(100) | No   | true  | NULL    |         |
| gender                 | VARCHAR(10)  | Yes  | false | NULL    | REPLACE |
| age                    | INT          | Yes  | false | NULL    | REPLACE |
| __DORIS_DELETE_SIGN__  | TINYINT      | No   | false | 0       | REPLACE |
| __DORIS_SEQUENCE_COL__ | INT          | Yes  | false | NULL    | REPLACE |
+------------------------+--------------+------+-------+---------+---------+
4 rows in set (0.00 sec)
```

Before load:
```text
+-------+--------+------+
| name  | gender | age  |
+-------+--------+------+
| li    | male   |   10 |
| wang  | male   |   14 |
| zhang | male   |   12 |
+-------+--------+------+
```

If you load data like this:
```text
li,male,10
```

After load:
```text
+-------+--------+------+
| name  | gender | age  |
+-------+--------+------+
| wang  | male   |   14 |
| zhang | male   |   12 |
+-------+--------+------+
```

You will find that the data is deleted.
```text
li,male,10
```

But if you load data like this:
```text
li,male,9
```

After load:
```text
+-------+--------+------+
| name  | gender | age  |
+-------+--------+------+
| li    | male   |   10 |
| wang  | male   |   14 |
| zhang | male   |   12 |
+-------+--------+------+
```

You will find that the data is not deleted.
```text
li,male,10
```
This is because in the underlying dependencies, it will first judge the case of the same key, display the row data with a large value in the sequence column, and then check whether the `__DORIS_DELETE_SIGN__` value of the row is 1. If it is 1, it will not be displayed. If it is 0, it will still be read out.

**When data is written and deleted at the same time in the imported data (such as in the Flink CDC scenario), using the sequence column can effectively ensure the consistency when the data arrives out of order, avoiding the deletion operation of an old version that arrives later, and accidentally deleting The new version of the data that arrives first.**