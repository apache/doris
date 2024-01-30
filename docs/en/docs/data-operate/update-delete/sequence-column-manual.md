---
{
    "title": "Sequence Column",
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

# Sequence Column
The sequence column currently only supports the Uniq model. The Uniq model is mainly aimed at scenarios that require a unique primary key, which can guarantee the uniqueness constraint of the primary key. However, due to the REPLACE aggregation method, the replacement order of data imported in the same batch is not guaranteed. See [Data Model](../../data-table/data-model.md). If the replacement order cannot be guaranteed, the specific data finally imported into the table cannot be determined, and there is uncertainty.

In order to solve this problem, Doris supports the sequence column. The user specifies the sequence column when importing. Under the same key column, the REPLACE aggregation type column will be replaced according to the value of the sequence column. The larger value can replace the smaller value, and vice versa. Cannot be replaced. This method leaves the determination of the order to the user, who controls the replacement order.

## Applicable Scene

Sequence columns can only be used under the Uniq data model.

## Fundamental

By adding a hidden column `__DORIS_SEQUENCE_COL__`, the type of the column is specified by the user when creating the table, the specific value of the column is determined during import, and the REPLACE column is replaced according to this value.

### Create Table
When creating a Uniq table, a hidden column `__DORIS_SEQUENCE_COL__` will be automatically added according to the user-specified type.

### Import

When importing, fe sets the value of the hidden column to the value of the `order by` expression (broker load and routine load), or the value of the `function_column.sequence_col` expression (stream load) during the parsing process, the value column will be Replace with this value. The value of the hidden column `__DORIS_SEQUENCE_COL__` can be set to either a column in the data source or a column in the table structure.

### Read

When the request contains the value column, the `__DORIS_SEQUENCE_COL__` column needs to be additionally read. This column is used as the basis for the replacement order of the REPLACE aggregate function under the same key column. The larger value can replace the smaller value, otherwise it cannot be replaced.

### Cumulative Compaction

The principle is the same as that of the reading process during Cumulative Compaction.

### Base Compaction

The principle is the same as the reading process during Base Compaction.

### Syntax

There are two ways to create a table with sequence column. One is to set the `sequence_col` attribute when creating a table, and the other is to set the `sequence_type` attribute when creating a table.

#### Set `sequence_col`(Recommend)
When you create the Uniq table, you can specify the mapping of sequence column to other columns

```text
PROPERTIES (
    "function_column.sequence_col" = 'column_name',
);
```
The sequence_col is used to specify the mapping of the sequence column to a column in the table, which can be integral and time (DATE, DATETIME). The type of this column cannot be changed after creation.

The import method is the same as that without the sequence column. It is relatively simple and recommended.

### Set `sequence_type`

When you create the Uniq table, you can specify the sequence column type

```text
PROPERTIES (
    "function_column.sequence_type" = 'Date',
);
```
The sequence_type is used to specify the type of the sequence column, which can be integral and time (DATE / DATETIME). 

The mapping column needs to be specified when importing.

#### Stream Load

The syntax of the stream load is to add the mapping of hidden columns corresponding to source_sequence in the `function_column.sequence_col` field in the header, for example

```shell
curl --location-trusted -u root -H "columns: k1,k2,source_sequence,v1,v2" -H "function_column.sequence_col: source_sequence" -T testData http://host:port/api/testDb/testTbl/_stream_load
```

#### Broker Load

Set the source_sequence field for the hidden column map at `ORDER BY`

```sql
LOAD LABEL db1.label1
(
    DATA INFILE("hdfs://host:port/user/data/*/test.txt")
    INTO TABLE `tbl1`
    COLUMNS TERMINATED BY ","
    (k1,k2,source_sequence,v1,v2)
    ORDER BY source_sequence
)
WITH BROKER 'broker'
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

The mapping method is the same as above, as shown below

```sql
   CREATE ROUTINE LOAD example_db.test1 ON example_tbl 
    [WITH MERGE|APPEND|DELETE]
    COLUMNS(k1, k2, source_sequence, v1, v2),
    WHERE k1 > 100 and k2 like "%doris%"
    [ORDER BY source_sequence]
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

## Enable Sequence Column Support
If `function_column.sequence_col`  or  `function_column.sequence_type` is set when creating a new table, the new table will support sequence column. For a table that does not support sequence column, if you want to use this function, you can use the following statement: `ALTER TABLE example_db.my_table ENABLE FEATURE "SEQUENCE_LOAD" WITH PROPERTIES ("function_column.sequence_type" = "Date")` to enable.

 If you are not sure whether a table supports sequence column, you can display hidden columns by setting a session variable `SET show_hidden_columns=true`, then use `desc tablename`, if there is a `__DORIS_SEQUENCE_COL__` column in the output, it is supported, if not, it is not supported .

## Usage Examples
Let's take the stream Load as an example to show how to use it
1. Create a table that supports sequence column. 

Create the test_table data table of the unique model and specify that the sequence column maps to the `modify_date` column in the table.

```sql
CREATE TABLE test.test_table
(
    user_id bigint,
    date date,
    group_id bigint,
    modify_date date,
    keyword VARCHAR(128)
)
UNIQUE KEY(user_id, date, group_id)
DISTRIBUTED BY HASH (user_id) BUCKETS 32
PROPERTIES(
    "function_column.sequence_col" = 'modify_date',
    "replication_num" = "1",
    "in_memory" = "false"
);
```

The table structure is shown below
```sql
MySQL > desc test_table;
+-------------+--------------+------+-------+---------+---------+
| Field       | Type         | Null | Key   | Default | Extra   |
+-------------+--------------+------+-------+---------+---------+
| user_id     | BIGINT       | No   | true  | NULL    |         |
| date        | DATE         | No   | true  | NULL    |         |
| group_id    | BIGINT       | No   | true  | NULL    |         |
| modify_date | DATE         | No   | false | NULL    | REPLACE |
| keyword     | VARCHAR(128) | No   | false | NULL    | REPLACE |
+-------------+--------------+------+-------+---------+---------+
```

2. Import data normally:

Import the following data
```
1       2020-02-22      1       2020-02-21      a
1       2020-02-22      1       2020-02-22      b
1       2020-02-22      1       2020-03-05      c
1       2020-02-22      1       2020-02-26      d
1       2020-02-22      1       2020-02-23      e
1       2020-02-22      1       2020-02-24      b
```
Take the Stream Load as an example here and map the sequence column to the modify_date column
```shell
curl --location-trusted -u root: -T testData http://host:port/api/test/test_table/_stream_load
```
The results is
```sql
MySQL > select * from test_table;
+---------+------------+----------+-------------+---------+
| user_id | date       | group_id | modify_date | keyword |
+---------+------------+----------+-------------+---------+
|       1 | 2020-02-22 |        1 | 2020-03-05  | c       |
+---------+------------+----------+-------------+---------+
```
In this import, the c is eventually retained in the keyword column because the value of the sequence column (the value in modify_date) is the maximum value: '2020-03-05'.

3. Guarantee of substitution order

After the above steps are completed, import the following data
```text
1       2020-02-22      1       2020-02-22      a
1       2020-02-22      1       2020-02-23      b
```
Query data
```sql
MySQL [test]> select * from test_table;
+---------+------------+----------+-------------+---------+
| user_id | date       | group_id | modify_date | keyword |
+---------+------------+----------+-------------+---------+
|       1 | 2020-02-22 |        1 | 2020-03-05  | c       |
+---------+------------+----------+-------------+---------+
```
In this import, the c is eventually retained in the keyword column because the value of the sequence column (the value in modify_date) in all imports is the maximum value: '2020-03-05'.
Try importing the following data again

```
1       2020-02-22      1       2020-02-22      a
1       2020-02-22      1       2020-03-23      w
```
Query data
```sql
MySQL [test]> select * from test_table;
+---------+------------+----------+-------------+---------+
| user_id | date       | group_id | modify_date | keyword |
+---------+------------+----------+-------------+---------+
|       1 | 2020-02-22 |        1 | 2020-03-23  | w       |
+---------+------------+----------+-------------+---------+
```
At this point, you can replace the original data in the table. To sum up, the sequence column will be compared among all the batches, the largest value of the same key will be imported into Doris table.

## Note
1. To prevent misuse, in load tasks like StreamLoad/BrokerLoad and row updates with insert statements, user must explicitly specify the sequence column (unless the sequence column's default value is CURRENT_TIMESTAMP); otherwise, user will receive the following error message:
```
Table test_tbl has a sequence column, need to specify the sequence column
```
2 Starting from version 2.0, Doris supports partial column updates for Merge-on-Write implementation on Unique Key tables. In load tasks with partial column update, users can update only a subset of columns at a time, so it is not mandatory to include the sequence column. If the import task submitted by the user includes the sequence column, it will have no effect on the behavior. However, if the import task does not include the sequence column, Doris will use the matching historical data's sequence column as the value for the updated row's sequence column. If there is no existing column with the same key in the historical data, it will be automatically filled with null or the default value.
