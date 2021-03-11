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
The Sequence Column currently only supports the Uniq model. The Uniq model is mainly for scenarios requiring a unique primary key, which can guarantee the uniqueness constraint of the primary key. However, due to the use of REPLACE aggregation, the replacement sequence is not guaranteed for data imported in the same batch, which can be described in detail [here](../../getting-started/data-model-rollup.md). If the order of substitution is not guaranteed, then the specific data that is finally imported into the table cannot be determined, and there is uncertainty.

To solve this problem, Doris supported a sequence column by allowing the user to specify the sequence column when importing. Under the same key column, columns of the REPLACE aggregate type will be replaced according to the value of the sequence column, larger values can be replaced with smaller values, and vice versa. In this method, the order is determined by the user, and the user controls the replacement order.

## Principle

Implemented by adding a hidden column `__DORIS_SEQUENCE_COL__`, the type of the column is specified by the user while create the table, determines the specific value of the column on import, and replaces the REPLACE column with that value.

### Create Table
When you create the Uniq table, a hidden column `__DORIS_SEQUENCE_COL__` is automatically added, depending on the type specified by the user

### Import

When importing, fe sets the value of the hidden column during parsing to the value of the 'order by' expression (Broker Load and routine Load), or the value of the 'function_column.sequence_col' expression (stream load), and the value column will be replaced according to this value. The value of the hidden column `__DORIS_SEQUENCE_COL__` can be set as a column in the source data or in the table structure.

### Read

The request with the value column needs to read the additional column of `__DORIS_SEQUENCE_COL__`, which is used as a basis for the order of replacement aggregation function replacement under the same key column, with the larger value replacing the smaller value and not the reverse.

### Cumulative Compaction

Cumulative Compaction works in the same way as the reading process

### Base Compaction

Base Compaction works in the same way as the reading process

### Syntax
The syntax aspect of the table construction adds a property to the property identifying the type of `__DORIS_SEQUENCE_COL__`.
The syntax design aspect of the import is primarily the addition of a mapping from the sequence column to other columns, the settings of each import mode are described below

#### Create Table
When you create the Uniq table, you can specify the sequence column type
```
PROPERTIES (
    "function_column.sequence_type" = 'Date',
);
```
The sequence_type is used to specify the type of the sequence column, which can be integral and time

#### stream load

The syntax of the stream load is to add the mapping of hidden columns corresponding to source_sequence in the 'function_column.sequence_col' field in the header, for example
```
curl --location-trusted -u root -H "columns: k1,k2,source_sequence,v1,v2" -H "function_column.sequence_col: source_sequence" -T testData http://host:port/api/testDb/testTbl/_stream_load
```

#### broker load

Set the source_sequence field for the hidden column map at `ORDER BY`

```
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

#### routine load

The mapping method is the same as above, as shown below

```
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

## Enable sequence column support
If `function_column.sequence_type` is set when creating a new table, then the sequence column will be supported.
For a table that does not support sequence column, use the following statement if you would like to use this feature:
`ALTER TABLE example_db.my_table ENABLE FEATURE "SEQUENCE_LOAD" WITH PROPERTIES ("function_column.sequence_type" = "Date")` to enable.
If you want to determine if a table supports sequence column, you can set the session variable to display the hidden column `SET show_hidden_columns=true`, followed by `desc Tablename`, if the output contains the column `__DORIS_SEQUENCE_COL__`, it is supported, if not, it is not supported

## Usage example
Let's take the stream Load as an example to show how to use it
1. Create a table that supports sequence column. 

The table structure is shown below
```
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
1       2020-02-22      1       2020-02-22      a
1       2020-02-22      1       2020-02-22      b
1       2020-02-22      1       2020-03-05      c
1       2020-02-22      1       2020-02-26      d
1       2020-02-22      1       2020-02-22      e
1       2020-02-22      1       2020-02-22      b
```
Take the Stream Load as an example here and map the sequence column to the modify_date column
```
curl --location-trusted -u root: -H "function_column.sequence_col: modify_date" -T testData http://host:port/api/test/test_table/_stream_load
```
The results is
```
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
```
1       2020-02-22      1       2020-02-22      a
1       2020-02-22      1       2020-02-23      b
```
Query data
```
MySQL [test]> select * from test_table;
+---------+------------+----------+-------------+---------+
| user_id | date       | group_id | modify_date | keyword |
+---------+------------+----------+-------------+---------+
|       1 | 2020-02-22 |        1 | 2020-03-05  | c       |
+---------+------------+----------+-------------+---------+
```
Because the sequence column for the newly imported data are all smaller than the values already in the table, they cannot be replaced
Try importing the following data again
```
1       2020-02-22      1       2020-02-22      a
1       2020-02-22      1       2020-03-23      w
```
Query data
```
MySQL [test]> select * from test_table;
+---------+------------+----------+-------------+---------+
| user_id | date       | group_id | modify_date | keyword |
+---------+------------+----------+-------------+---------+
|       1 | 2020-02-22 |        1 | 2020-03-23  | w       |
+---------+------------+----------+-------------+---------+
```
At this point, you can replace the original data in the table