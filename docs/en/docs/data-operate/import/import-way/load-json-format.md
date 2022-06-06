---
{
    "title": "Load Json Format Data",
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

# JSON format data import

Doris supports importing data in JSON format. This document mainly describes the precautions when importing data in JSON format.

## Supported import methods

Currently, only the following import methods support data import in Json format:

- Import the local JSON format file through [STREAM LOAD](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/STREAM-LOAD.md).
- Subscribe and consume JSON format in Kafka via [ROUTINE LOAD](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/CREATE-ROUTINE-LOAD.md) information.

Other ways of importing data in JSON format are not currently supported.

## Supported Json formats

Currently only the following two Json formats are supported:

1. Multiple rows of data represented by Array

   Json format with Array as root node. Each element in the Array represents a row of data to be imported, usually an Object. An example is as follows:

   ````json
   [
       { "id": 123, "city" : "beijing"},
       { "id": 456, "city" : "shanghai"},
       ...
   ]
   ````

   ````json
   [
       { "id": 123, "city" : { "name" : "beijing", "region" : "haidian"}},
       { "id": 456, "city" : { "name" : "beijing", "region" : "chaoyang"}},
       ...
   ]
   ````

   This method is typically used for Stream Load import methods to represent multiple rows of data in a batch of imported data.

   This method must be used with the setting `strip_outer_array=true`. Doris will expand the array when parsing, and then parse each Object in turn as a row of data.

2. A single row of data represented by Object

   Json format with Object as root node. The entire Object represents a row of data to be imported. An example is as follows:

   ````json
   { "id": 123, "city" : "beijing"}
   ````

   ````json
   { "id": 123, "city" : { "name" : "beijing", "region" : "haidian" }}
   ````

   This method is usually used for the Routine Load import method, such as representing a message in Kafka, that is, a row of data.

### fuzzy_parse parameters

In [STREAM LOAD](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/STREAM-LOAD.md) `fuzzy_parse` parameter can be added to speed up JSON Data import efficiency.

This parameter is usually used to import the format of **multi-line data represented by Array**, so it is generally used with `strip_outer_array=true`.

This feature requires that each row of data in the Array has exactly the same order of fields. Doris will only parse according to the field order of the first row, and then access the subsequent data in the form of subscripts. This method can improve the import efficiency by 3-5X.

## Json Path

Doris supports extracting data specified in Json through Json Path.

**Note: Because for Array type data, Doris will expand the array first, and finally process it in a single line according to the Object format. So the examples later in this document are all explained with Json data in a single Object format. **

- do not specify Json Path

  If Json Path is not specified, Doris will use the column name in the table to find the element in Object by default. An example is as follows:

  The table contains two columns: `id`, `city`

  The Json data is as follows:

  ````json
  { "id": 123, "city" : "beijing"}
  ````

  Then Doris will use `id`, `city` for matching, and get the final data `123` and `beijing`.

  If the Json data is as follows:

  ````json
  { "id": 123, "name" : "beijing"}
  ````

  Then use `id`, `city` for matching, and get the final data `123` and `null`.

- Specify Json Path

  Specify a set of Json Path in the form of a Json data. Each element in the array represents a column to extract. An example is as follows:

  ````json
  ["$.id", "$.name"]
  ````

  ````json
  ["$.id.sub_id", "$.name[0]", "$.city[0]"]
  ````

  Doris will use the specified Json Path for data matching and extraction.

- matches non-primitive types

  The values that are finally matched in the preceding examples are all primitive types, such as integers, strings, and so on. Doris currently does not support composite types, such as Array, Map, etc. So when a non-basic type is matched, Doris will convert the type to a string in Json format and import it as a string type. An example is as follows:

  The Json data is:

  ````json
  { "id": 123, "city" : { "name" : "beijing", "region" : "haidian" }}
  ````

  Json Path is `["$.city"]`. The matched elements are:

  ````json
  { "name" : "beijing", "region" : "haidian" }
  ````

  The element will be converted to a string for subsequent import operations:

  ````json
  "{'name':'beijing','region':'haidian'}"
  ````

- match failed

  When the match fails, `null` will be returned. An example is as follows:

  The Json data is:

  ````json
  { "id": 123, "name" : "beijing"}
  ````

  Json Path is `["$.id", "$.info"]`. The matched elements are `123` and `null`.

  Doris currently does not distinguish between null values represented in Json data and null values produced when a match fails. Suppose the Json data is:

  ````json
  { "id": 123, "name" : null }
  ````

  The same result would be obtained with the following two Json Paths: `123` and `null`.

  ````json
  ["$.id", "$.name"]
  ````

  ````json
  ["$.id", "$.info"]
  ````

- Exact match failed

  In order to prevent misoperation caused by some parameter setting errors. When Doris tries to match a row of data, if all columns fail to match, it considers this to be an error row. Suppose the Json data is:

  ````json
  { "id": 123, "city" : "beijing" }
  ````

  If the Json Path is written incorrectly as (or if the Json Path is not specified, the columns in the table do not contain `id` and `city`):

  ````json
  ["$.ad", "$.infa"]
  ````

  would cause the exact match to fail, and the line would be marked as an error line instead of yielding `null, null`.

## Json Path and Columns

Json Path is used to specify how to extract data in JSON format, while Columns specifies the mapping and conversion relationship of columns. Both can be used together.

In other words, it is equivalent to rearranging the columns of a Json format data according to the column order specified in Json Path through Json Path. After that, you can map the rearranged source data to the columns of the table through Columns. An example is as follows:

Data content:

````json
{"k1": 1, "k2": 2}
````

Table Structure:

````
k2 int, k1 int
````

Import statement 1 (take Stream Load as an example):

```bash
curl -v --location-trusted -u root: -H "format: json" -H "jsonpaths: [\"$.k2\", \"$.k1\"]" -T example.json http:/ /127.0.0.1:8030/api/db1/tbl1/_stream_load
````

In import statement 1, only Json Path is specified, and Columns is not specified. The role of Json Path is to extract the Json data in the order of the fields in the Json Path, and then write it in the order of the table structure. The final imported data results are as follows:

````text
+------+------+
| k1 | k2 |
+------+------+
| 2 | 1 |
+------+------+
````

You can see that the actual k1 column imports the value of the "k2" column in the Json data. This is because the field name in Json is not equivalent to the field name in the table structure. We need to explicitly specify the mapping between the two.

Import statement 2:

```bash
curl -v --location-trusted -u root: -H "format: json" -H "jsonpaths: [\"$.k2\", \"$.k1\"]" -H "columns: k2, k1 " -T example.json http://127.0.0.1:8030/api/db1/tbl1/_stream_load
````

Compared with the import statement 1, the Columns field is added here to describe the mapping relationship of the columns, in the order of `k2, k1`. That is, after extracting in the order of fields in Json Path, specify the value of column k2 in the table for the first column, and the value of column k1 in the table for the second column. The final imported data results are as follows:

````text
+------+------+
| k1 | k2 |
+------+------+
| 1 | 2 |
+------+------+
````

Of course, as with other imports, column transformations can be performed in Columns. An example is as follows:

```bash
curl -v --location-trusted -u root: -H "format: json" -H "jsonpaths: [\"$.k2\", \"$.k1\"]" -H "columns: k2, tmp_k1 , k1 = tmp_k1 * 100" -T example.json http://127.0.0.1:8030/api/db1/tbl1/_stream_load
````

The above example will import the value of k1 multiplied by 100. The final imported data results are as follows:

````text
+------+------+
| k1 | k2 |
+------+------+
| 100 | 2 |
+------+------+
````

## NULL and Default values

Example data is as follows:

````json
[
    {"k1": 1, "k2": "a"},
    {"k1": 2},
    {"k1": 3, "k2": "c"},
]
````


The table structure is: `k1 int null, k2 varchar(32) null default "x"`

The import statement is as follows:

```bash
curl -v --location-trusted -u root: -H "format: json" -H "strip_outer_array: true" -T example.json http://127.0.0.1:8030/api/db1/tbl1/_stream_load
````

The import results that users may expect are as follows, that is, for missing columns, fill in the default values.

````text
+------+------+
| k1 | k2 |
+------+------+
| 1 | a |
+------+------+
| 2 | x |
+------+------+
|3|c|
+------+------+
````

But the actual import result is as follows, that is, for the missing column, NULL is added.

````text
+------+------+
| k1 | k2 |
+------+------+
| 1 | a |
+------+------+
| 2 | NULL |
+------+------+
|3|c|
+------+------+
````

This is because Doris doesn't know "the missing column is column k2 in the table" from the information in the import statement. If you want to import the above data according to the expected result, the import statement is as follows:

```bash
curl -v --location-trusted -u root: -H "format: json" -H "strip_outer_array: true" -H "jsonpaths: [\"$.k1\", \"$.k2\"]" - H "columns: k1, tmp_k2, k2 = ifnull(tmp_k2, 'x')" -T example.json http://127.0.0.1:8030/api/db1/tbl1/_stream_load
````

## Application example

### Stream Load

Because of the inseparability of the Json format, when using Stream Load to import a Json format file, the file content will be fully loaded into the memory before processing begins. Therefore, if the file is too large, it may take up more memory.

Suppose the table structure is:

````text
id INT NOT NULL,
city VARHCAR NULL,
code INT NULL
````

1. Import a single row of data 1

   ````json
   {"id": 100, "city": "beijing", "code" : 1}
   ````

   - do not specify Json Path

     ```bash
     curl --location-trusted -u user:passwd -H "format: json" -T data.json http://localhost:8030/api/db1/tbl1/_stream_load
     ````

     Import result:

     ````text
     100 beijing 1
     ````

   - Specify Json Path

     ```bash
     curl --location-trusted -u user:passwd -H "format: json" -H "jsonpaths: [\"$.id\",\"$.city\",\"$.code\"]" - T data.json http://localhost:8030/api/db1/tbl1/_stream_load
     ````

     Import result:

     ````text
     100 beijing 1
     ````

2. Import a single row of data 2

   ````json
   {"id": 100, "content": {"city": "beijing", "code": 1}}
   ````

   - Specify Json Path

     ```bash
     curl --location-trusted -u user:passwd -H "format: json" -H "jsonpaths: [\"$.id\",\"$.content.city\",\"$.content.code\ "]" -T data.json http://localhost:8030/api/db1/tbl1/_stream_load
     ````

     Import result:

     ````text
     100 beijing 1
     ````

3. Import multiple rows of data

   ````json
   [
       {"id": 100, "city": "beijing", "code" : 1},
       {"id": 101, "city": "shanghai"},
       {"id": 102, "city": "tianjin", "code" : 3},
       {"id": 103, "city": "chongqing", "code" : 4},
       {"id": 104, "city": ["zhejiang", "guangzhou"], "code" : 5},
       {
           "id": 105,
           "city": {
               "order1": ["guangzhou"]
           },
           "code" : 6
       }
   ]
   ````

   - Specify Json Path

     ```bash
     curl --location-trusted -u user:passwd -H "format: json" -H "jsonpaths: [\"$.id\",\"$.city\",\"$.code\"]" - H "strip_outer_array: true" -T data.json http://localhost:8030/api/db1/tbl1/_stream_load
     ````

     Import result:

     ````text
     100 beijing 1
     101 shanghai NULL
     102 tianjin 3
     103 chongqing 4
     104 ["zhejiang","guangzhou"] 5
     105 {"order1":["guangzhou"]} 6
     ````

4. Transform the imported data

   The data is still the multi-line data in Example 3, and now it is necessary to add 1 to the `code` column in the imported data before importing.

   ```bash
   curl --location-trusted -u user:passwd -H "format: json" -H "jsonpaths: [\"$.id\",\"$.city\",\"$.code\"]" - H "strip_outer_array: true" -H "columns: id, city, tmpc, code=tmpc+1" -T data.json http://localhost:8030/api/db1/tbl1/_stream_load
   ````

   Import result:

   ````text
   100 beijing 2
   101 shanghai NULL
   102 tianjin 4
   103 chongqing 5
   104 ["zhejiang","guangzhou"] 6
   105 {"order1":["guangzhou"]} 7
   ````

### Routine Load

The processing principle of Routine Load for Json data is the same as that of Stream Load. It is not repeated here.

For Kafka data sources, the content in each Massage is treated as a complete Json data. If there are multiple rows of data represented in Array format in a Massage, multiple rows will be imported, and the offset of Kafka will only increase by 1. If an Array format Json represents multiple lines of data, but the Json parsing fails due to the wrong Json format, the error line will only increase by 1 (because the parsing fails, in fact, Doris cannot determine how many lines of data are contained in it, and can only error by one line data record)
