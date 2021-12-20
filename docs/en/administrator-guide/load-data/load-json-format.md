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

# Load Json Format Data

Doris supports data load in Json format since version 0.12.

## Supported Load Methods

Currently only the following load methods support data import in Json format:

* Stream Load
* Routine Load

For specific instructions on the above load methods, please refer to the relevant documentation. This document mainly introduces the instructions for using Json in these load methods.

## Supported Json Format

Currently, only the following two Json formats are supported:

1. Multi-line data represented by Array

    Json format with Array as the root node. Each element in the Array represents a row of data to be loaded, usually an Object. Examples are as follows:

    ```
    [
        { "id": 123, "city" : "beijing"},
        { "id": 456, "city" : "shanghai"},
        ...
    ]
    ```
    
    ```
    [
        { "id": 123, "city" : { "name" : "beijing", "region" : "haidian"}},
        { "id": 456, "city" : { "name" : "beijing", "region" : "chaoyang"}},
        ...
    ]
    ```
    
    This method is usually used for the Stream Load method to represent multiple rows of data in a batch of load data.
    
     This method must be used in conjunction with setting `stripe_outer_array=true`. Doris will expand the array when parsing, and then parse each Object in turn as a row of data.

2. Single row of data represented by Object

    Json format with Object as the root node. The entire Object represents a row of data to be loaded. Examples are as follows:
    
    ```
    { "id": 123, "city" : "beijing"}
    ```
    
    ```
    { "id": 123, "city" : { "name" : "beijing", "region" : "haidian" }}
    ```
    
    This method is usually used for the Routine Load method, such as representing a message in Kafka, that is, a row of data.
        
## Json Path

Doris supports extracting the data specified in Json through Json Path.

**Note: Because for Array type data, Doris will first expand the array, and finally perform single-line processing according to the Object format. Therefore, the examples after this document will be illustrated with Json data in single Object format.**

* Json Path is not specified

    If Json Path is not specified, Doris will use the column names in the table to find the elements in Object by default. Examples are as follows:
    
    The table contains two columns: `id`, `city`
    
    Json data is as follows:
    
    ```
    { "id": 123, "city" : "beijing"}
    ```
    
    Then Doris will use `id`, `city` to match, and get the final data `123` and `beijing`.
    
    If the Json data is as follows:
    
    ```
    { "id": 123, "name" : "beijing"}
    ```

    Then use `id`, `city` to match and get the final data `123` and `null`.
    
* Json Path is specified

    Specify a set of Json Path in the form of a Json data. Each element in the array represents a column to be extracted. Examples are as follows:
    
    ```
    ["$.id", "$.name"]
    ```
    ```
    ["$.id.sub_id", "$.name[0]", "$.city[0]"]
    ```
    
    Doris will use the specified Json Path for data matching and extraction.
    
* Match non-primitive types

     The values that the previous example finally matched are all primitive types, such as Integer, String, and so on. Doris currently does not support complex types, such as Array, Map, etc. So when a non-primitive type is matched, Doris will convert the type to a Json format string and load it as a string type. Examples are as follows:
        
    ```
    { "id": 123, "city" : { "name" : "beijing", "region" : "haidian" }}
    ```
    
    The Json Path is `["$.city"]`. Then the matched elements are:
    
    ```
    { "name" : "beijing", "region" : "haidian" }
    ```
    
    This element will be converted into a string for subsequent load operations:
    
    ```
    "{'name':'beijing','region':'haidian'}"
    ```

* Match failed

    When the match fails, `null` will be returned. Examples are as follows:
    
    Json data is:
    
    ```
    { "id": 123, "name" : "beijing"}
    ```
    
    The Json Path is `["$.id", "$.info"]`. Then the matched elements are `123` and `null`.
    
     Doris currently does not distinguish between the null value represented in the Json data and the null value generated when the match fails. Suppose the Json data is:
    
    ```
    { "id": 123, "name" : null }
    ```
    
    Then use the following two Json Path will get the same result: `123` and `null`.
        
    ```
    ["$.id", "$.name"]
    ```
    ```
    ["$.id", "$.info"]
    ```
    
* Complete match failed

    In order to prevent misoperation caused by some parameter setting errors. When Doris tries to match a row of data, if all columns fail to match, it will be considered a error row. Suppose the Json data is:
    
    ```
    { "id": 123, "city" : "beijing" }
    ```
    
    If Json Path is incorrectly written as (or when Json Path is not specified, the columns in the table do not contain `id` and `city`):
    
    ```
    ["$.ad", "$.infa"]
    ```
    
    Will result in a complete match failure, the line will be marked as an error row, instead of producing `null, null`.

## Json Path and Columns

Json Path is used to specify how to extract data in JSON format, and Columns specify the mapping and conversion relationship of columns. The two can be used together.

In other words, it is equivalent to using Json Path to rearrange the data in a Json format according to the column order specified in Json Path. After that, you can use Columns to map the rearranged source data to the columns of the table. Examples are as follows:

Data content:

```
{"k1": 1, "k2": 2}
```

Table schema:

`k2 int, k1 int`

Load statement 1 (take Stream Load as an example):

```
curl -v --location-trusted -u root: -H "format: json" -H "jsonpaths: [\"$.k2\", \"$.k1\"]" -T example.json http:/ /127.0.0.1:8030/api/db1/tbl1/_stream_load
```

In Load statement 1, only Json Path is specified, and Columns are not specified. The role of Json Path is to extract the Json data in the order of the fields in the Json Path, and then write it in the order of the table schema. The final loaded data results are as follows:

```
+------+------+
| k1   | k2   |
+------+------+
|    2 |    1 |
+------+------+
```

You will see that the actual k1 column has loaded the value of the "k2" column in the Json data. This is because the field name in Json is not equivalent to the field name in the table schema. We need to explicitly specify the mapping relationship between the two.

Load statement 2:

```
curl -v --location-trusted -u root: -H "format: json" -H "jsonpaths: [\"$.k2\", \"$.k1\"]" -H "columns: k2, k1 "-T example.json http://127.0.0.1:8030/api/db1/tbl1/_stream_load
```

Compared to load statement 1, here is the Columns field, which is used to describe the mapping relationship of columns, in the order of `k2, k1`. That is, after extracting in the order of the fields in the Json Path, specify the first column as the value of the k2 column in the table, and the second column as the value of the k1 column in the table. The final loaded data results are as follows:

```
+------+------+
| k1   | k2   |
+------+------+
|    1 |    2 |
+------+------+
```

Of course, like other load methods, you can perform column conversion operations in Columns. Examples are as follows:

```
curl -v --location-trusted -u root: -H "format: json" -H "jsonpaths: [\"$.k2\", \"$.k1\"]" -H "columns: k2, tmp_k1 , k1 = tmp_k1 * 100" -T example.json http://127.0.0.1:8030/api/db1/tbl1/_stream_load
```

The above example will multiply the value of k1 by 100 and import it. The final imported data results are as follows:

```
+------+------+
| k1   | k2   |
+------+------+
|  100 |    2 |
+------+------+
```

## NULL and Default value

The sample data is as follows:

```
[
    {"k1": 1, "k2": "a"},
    {"k1": 2},
    {"k1": 3, "k2": "c"},
]
```

The table schema is: `k1 int null, k2 varchar(32) null default "x"`

The load statement is as follows:

```
curl -v --location-trusted -u root: -H "format: json" -H "strip_outer_array: true" -T example.json http://127.0.0.1:8030/api/db1/tbl1/_stream_load
```

The import results that users may expect are as follows, that is, for missing columns, fill in default values.

```
+------+------+
| k1   | k2   |
+------+------+
|    1 |    a |
+------+------+
|    2 |    x |
+------+------+
|    3 |    c |
+------+------+
```

But the actual load result is as follows, that is, for missing columns, NULL is added.

```
+------+------+
| k1   | k2   |
+------+------+
|    1 |    a |
+------+------+
|    2 | NULL |
+------+------+
|    3 |    c |
+------+------+
```

This is because through the information in the load statement, Doris does not know that "the missing column is the k2 column in the table".
If you want to load the above data as expected, the load statement is as follows:

```
curl -v --location-trusted -u root: -H "format: json" -H "strip_outer_array: true" -H "jsonpaths: [\"$.k1\", \"$.k2\"]"- H "columns: k1, tmp_k2, k2 = ifnull(tmp_k2,'x')" -T example.json http://127.0.0.1:8030/api/db1/tbl1/_stream_load
```

## LargetInt and Decimal

Doris supports data types such as largeint and decimal with larger data range and higher data precision. However, due to the fact that the maximum range of the rapid JSON library used by Doris for the resolution of digital types is Int64 and double, there may be some problems when importing largeint or decimal by JSON format,  such as loss of precision, data conversion error, etc.

For example:

```
[
    {"k1": 1, "k2":9999999999999.999999 }
]
```


The imported K2 column type is `Decimal (16,9)`the import data is: ` 9999999999.999999`. During the JSON load which cause the precision loss of double conversion, the imported data convert to: ` 10000000000.0002 `. It is a import error.

To solve this problem, Doris provides a param `num_as_string `. Doris converts the numeric type to a string when parsing JSON data and JSON load without losing precision.

```
curl -v --location-trusted -u root: -H "format: json" -H "num_as_string: true" -T example.json http://127.0.0.1:8030/api/db1/tbl1/_stream_load
```

But using the param will cause unexpected side effects. Doris currently does not support composite types, such as Array, Map, etc. So when a non basic type is matched, Doris will convert the type to a string in JSON format.` num_as_string`will also convert compound type numbers into strings, for example:
    
JSON Data:

    { "id": 123, "city" : { "name" : "beijing", "city_id" : 1 }}

Not use `num_as_string `, the data of the city column is:

`{ "name" : "beijing", "city_id" : 1 }`

Use `num_as_string `, the data of the city column is:

`{ "name" : "beijing", "city_id" : "1" }`

Warning, the param leads to the city_id of the numeric type in the compound type is treated as a string column and quoted, which is different from the original data.

Therefore, when using JSON load. we should try to avoid importing largeint, decimal and composite types at the same time. If you can't avoid it, you need to fully understand the **side effects**.

## Examples

### Stream Load

Because of the indivisible nature of the Json format, when using Stream Load to load a Json format file, the file content will be fully loaded into memory before processing. Therefore, if the file is too large, it may occupy more memory.

Suppose the table structure is:

```
id      INT     NOT NULL,
city    VARHCAR NULL,
code    INT     NULL
```

1. Load single-line data 1

    ```
    {"id": 100, "city": "beijing", "code" : 1}
    ```

    * Not specify Json Path
    
        ```
        curl --location-trusted -u user:passwd -H "format: json" -T data.json http://localhost:8030/api/db1/tbl1/_stream_load
        ```
        
        Results:
        
        ```
        100     beijing     1
        ```
        
    * Specify Json Path

        ```
        curl --location-trusted -u user:passwd -H "format: json" -H "jsonpaths: [\"$.id\",\"$.city\",\"$.code\"]" -T data.json http://localhost:8030/api/db1/tbl1/_stream_load
        ```
        
        Results:
        
        ```
        100     beijing     1
        ```
    
2. Load sigle-line data 2

    ```
    {"id": 100, "content": {"city": "beijing", "code" : 1}}
    ```

    * Specify Json Path

        ```
        curl --location-trusted -u user:passwd -H "format: json" -H "jsonpaths: [\"$.id\",\"$.content.city\",\"$.content.code\"]" -T data.json http://localhost:8030/api/db1/tbl1/_stream_load
        ```

        Results:
        
        ```
        100     beijing     1
        ```

3. Load multi-line data

    ```
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
    ```

    * Specify Json Path

        ```
        curl --location-trusted -u user:passwd -H "format: json" -H "jsonpaths: [\"$.id\",\"$.city\",\"$.code\"]" -H "strip_outer_array: true" -T data.json http://localhost:8030/api/db1/tbl1/_stream_load
        ```

        Results:
        
        ```
        100     beijing                     1
        101     shanghai                    NULL
        102     tianjin                     3
        103     chongqing                   4
        104     ["zhejiang","guangzhou"]    5
        105     {"order1":["guangzhou"]}    6
        ```
    
4. Convert load data

    The data is still the multi-row data in Example 3. Now you need to add 1 to the `code` column in the loaded data and load it.
    
    ```
    curl --location-trusted -u user:passwd -H "format: json" -H "jsonpaths: [\"$.id\",\"$.city\",\"$.code\"]" -H "strip_outer_array: true" -H "columns: id, city, tmpc, code=tmpc+1" -T data.json http://localhost:8030/api/db1/tbl1/_stream_load
    ```

    Results:
        
    ```
    100     beijing                     2
    101     shanghai                    NULL
    102     tianjin                     4
    103     chongqing                   5
    104     ["zhejiang","guangzhou"]    6
    105     {"order1":["guangzhou"]}    7
    ```

### Routine Load

Routine Load processes Json data the same as Stream Load. I will not repeat them here.

For the Kafka data source, the content of each Massage is treated as a complete Json data. If multiple rows of data expressed in Array format in a Massage are loaded, multiple rows will be loaded, and Kafka's offset will only increase by 1. If an Array format Json represents multiple rows of data, but because the Json format error causes the parsing Json to fail, the error row will only increase by 1 (because the parsing fails, in fact, Doris cannot determine how many rows of data it contains, and can only add one row of errors rows record).
