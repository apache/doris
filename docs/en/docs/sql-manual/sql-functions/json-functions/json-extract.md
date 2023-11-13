---
{
    "title": "JSON_EXTRACT",
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

## json_extract

<version since="dev"></version>

### description

#### Syntax

```sql
`VARCHAR json_extract(VARCHAR json_str, VARCHAR path[, VARCHAR path] ...))`
JSON jsonb_extract(JSON j, VARCHAR json_path)
BOOLEAN json_extract_isnull(JSON j, VARCHAR json_path)
BOOLEAN json_extract_bool(JSON j, VARCHAR json_path)
INT json_extract_int(JSON j, VARCHAR json_path)
BIGINT json_extract_bigint(JSON j, VARCHAR json_path)
LARGEINT json_extract_largeint(JSON j, VARCHAR json_path)
DOUBLE json_extract_double(JSON j, VARCHAR json_path)
STRING json_extract_string(JSON j, VARCHAR json_path)
```

json_extract functions extract field specified by json_path from JSON. A series of functions are provided for different datatype.
- json_extract with VARCHAR argument, extract and return VARCHAR datatype
- jsonb_extract extract and return JSON datatype
- json_extract_isnull check if the field is json null and return BOOLEAN datatype
- json_extract_bool extract and return BOOLEAN datatype
- json_extract_int extract and return INT datatype
- json_extract_bigint extract and return BIGINT datatype
- json_extract_largeint extract and return LARGEINT datatype
- json_extract_double extract and return DOUBLE datatype
- json_extract_STRING extract and return STRING datatype

json path syntax:
- '$' for json document root
- '.k1' for element of json object with key 'k1'
  - If the key column value contains ".", double quotes are required in json_path, For example: SELECT json_extract('{"k1.a":"abc","k2":300}', '$."k1.a"');
- '[i]' for element of json array at index i
  - Use '$[last]' to get the last element of json_array, and '$[last-1]' to get the penultimate element, and so on.


Exception handling is as follows:
- if the field specified by json_path does not exist, return NULL
- if datatype of the field specified by json_path is not the same with type of json_extract_t, return t if it can be cast to t else NULL


## json_exists_path and json_type
### description

#### Syntax

```sql
BOOLEAN json_exists_path(JSON j, VARCHAR json_path)
STRING json_type(JSON j, VARCHAR json_path)
```

There are two extra functions to check field existence and type
- json_exists_path check the existence of the field specified by json_path, return TRUE or FALS
- json_type get the type as follows of the field specified by json_path, return NULL if it does not exist
  - object
  - array
  - null
  - bool
  - int
  - bigint
  - largeint
  - double
  - string

### example

refer to [json tutorial](../../sql-reference/Data-Types/JSON.md) for more.

```
mysql> SELECT json_extract('{"id": 123, "name": "doris"}', '$.id');
+------------------------------------------------------+
| json_extract('{"id": 123, "name": "doris"}', '$.id') |
+------------------------------------------------------+
| 123                                                  |
+------------------------------------------------------+
1 row in set (0.01 sec)

mysql> SELECT json_extract('[1, 2, 3]', '$.[1]');
+------------------------------------+
| json_extract('[1, 2, 3]', '$.[1]') |
+------------------------------------+
| 2                                  |
+------------------------------------+
1 row in set (0.01 sec)

mysql> SELECT json_extract('{"k1": "v1", "k2": { "k21": 6.6, "k22": [1, 2] } }', '$.k1', '$.k2.k21', '$.k2.k22', '$.k2.k22[1]');
+-------------------------------------------------------------------------------------------------------------------+
| json_extract('{"k1": "v1", "k2": { "k21": 6.6, "k22": [1, 2] } }', '$.k1', '$.k2.k21', '$.k2.k22', '$.k2.k22[1]') |
+-------------------------------------------------------------------------------------------------------------------+
| ["v1",6.6,[1,2],2]                                                                                                |
+-------------------------------------------------------------------------------------------------------------------+
1 row in set (0.01 sec)

mysql> SELECT json_extract('{"id": 123, "name": "doris"}', '$.aaa', '$.name');
+-----------------------------------------------------------------+
| json_extract('{"id": 123, "name": "doris"}', '$.aaa', '$.name') |
+-----------------------------------------------------------------+
| [null,"doris"]                                                  |
+-----------------------------------------------------------------+
1 row in set (0.01 sec)
```


### keywords
JSONB, JSON, json_extract, json_extract_isnull, json_extract_bool, json_extract_int, json_extract_bigint, json_extract_largeint,json_extract_double, json_extract_string, json_exists_path, json_type