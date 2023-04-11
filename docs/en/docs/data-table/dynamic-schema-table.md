---
{
    "title": "[Experimental] Dynamie schema table",
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

# Dynamic Table

<version since="2.0.0">

Dynamic Table

</version>


A dynamic schema table is a special kind of table which schema expands automatically with the import procedure. Currently, this feature is mainly used for importing semi-structured data such as JSON. Because JSON is self-describing, we can extract the schema information from the original document and infer the final type information. This special table can reduce manual schema change operations and easily import semi-structured data and automatically expand its schema.

## Terminology
- Schema change, changing the structure of the table, such as adding columns, reducing columns, changing column types
- Static column, column specified during table creation, such as partition columns, primary key columns
- Dynamic column, columns automatically recognized and added during import

## Create dynamic table

```sql
CREATE DATABASE test_dynamic_table;

-- Create table and specify static column types, import will automatically convert to the type of static column
-- Choose random bucketing
CREATE TABLE IF NOT EXISTS test_dynamic_table (
                qid bigint,
                `answers.date` array<datetime>,
                `title` string,
		        ...   -- ... Identifying a table as a dynamic table and its syntax for dynamic tables.
        )
DUPLICATE KEY(`qid`)
DISTRIBUTED BY RANDOM BUCKETS 5 
properties("replication_num" = "1");

-- Three Columns are added to the table by default, and their types are specified
mysql> DESC test_dynamic_table;
+--------------+-----------------+------+-------+---------+-------+
| Field        | Type            | Null | Key   | Default | Extra |
+--------------+-----------------+------+-------+---------+-------+
| qid          | BIGINT          | Yes  | true  | NULL    |       |
| answers.date | ARRAY<DATETIME> | Yes  | false | NULL    | NONE  |
| user         | TEXT            | Yes  | false | NULL    | NONE  |
+--------------+-----------------+------+-------+---------+-------+
3 rows in set (0.00 sec)
```

## Importing data

``` sql
-- example1.json
'{
    "title": "Display Progress Bar at the Time of Processing",
    "qid": "1000000",
    "answers": [
        {"date": "2009-06-16T09:55:57.320", "user": "Micha\u0142 Niklas (22595)"},
        {"date": "2009-06-17T12:34:22.643", "user": "Jack Njiri (77153)"}
    ],
    "tag": ["vb6", "progress-bar"],
    "user": "Jash",
    "creationdate": "2009-06-16T07:28:42.770"
}'

curl -X PUT -T example1.json --location-trusted -u root: -H "read_json_by_line:false" -H "format:json"   http://127.0.0.1:8147/api/regression_test_dynamic_table/test_dynamic_table/_stream_load

-- Added five new columns: `title`, `answers.user`, `tag`, `title`, `creationdate`
-- The types of the three columns: `qid`, `answers.date`, `user` remain the same as with the table was created
-- The default value of the new array type is an empty array []
mysql> DESC test_dynamic_table;                                                                                 
+--------------+-----------------+------+-------+---------+-------+
| Field        | Type            | Null | Key   | Default | Extra |
+--------------+-----------------+------+-------+---------+-------+
| qid          | BIGINT          | Yes  | true  | NULL    |       |
| answers.date | ARRAY<DATETIME> | Yes  | false | NULL    | NONE  |
| title        | TEXT            | Yes  | false | NULL    | NONE  |
| answers.user | ARRAY<TEXT>     | No   | false | []      | NONE  |
| tag          | ARRAY<TEXT>     | No   | false | []      | NONE  |
| user         | TEXT            | Yes  | false | NULL    | NONE  |
| creationdate | TEXT            | Yes  | false | NULL    | NONE  |
| date         | TEXT            | Yes  | false | NULL    | NONE  |
+--------------+-----------------+------+-------+---------+-------+

-- Batch import data
-- Specifying -H "read_json_by_line:true", parsing JSON line by line
curl -X PUT -T example_batch.json --location-trusted -u root: -H "read_json_by_line:true" -H "format:json"   http://127.0.0.1:8147/api/regression_test_dynamic_table/test_dynamic_table/_stream_load

-- Specifying -H "strip_outer_array:true", parsing the entire file as a JSON array, each element in the array is the same, more efficient parsing way
curl -X PUT -T example_batch_array.json --location-trusted -u root: -H "strip_outer_array:true" -H "format:json"   http://127.0.0.1:8147/api/regression_test_dynamic_table/test_dynamic_table/_stream_load
```
For a dynamic table, you can also use S3load or Routine load, with similar usage.


## Adding Index to Dynamic Columns
```sql
-- Create an inverted index on the title column, using English parsing.
CREATE INDEX title_idx ON test_dynamic_table (`title`) using inverted PROPERTIES("parser"="english")
```

## Type conflict resolution

In the first batch import, the unified type will be automatically inferred and used as the final Column type, so it is recommended to keep the Column type consistent, for example:
```
{"id" : 123}
{"id" : "123"}
-- The type will finally be inferred as Text type, and if {"id" : 123} is imported later, the type will automatically be converted to String type

For types that cannot be unified, such as:
{"id" : [123]}
{"id" : 123}
-- Importing will result in an error."
```