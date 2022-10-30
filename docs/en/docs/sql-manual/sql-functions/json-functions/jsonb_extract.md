---
{
    "title": "jsonb_extract",
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

## jsonb_extract
### description

jsonb_extract functions extract field specified by json_path from JSONB. A series of functions are provided for different datatype.
- jsonb_extract extract and return JSONB datatype
- jsonb_extract_isnull check if the field is json null and return BOOLEAN datatype
- jsonb_extract_bool extract and return BOOLEAN datatype
- jsonb_extract_int extract and return INT datatype
- jsonb_extract_bigint extract and return BIGINT datatype
- jsonb_extract_double extract and return DOUBLE datatype
- jsonb_extract_STRING extract and return STRING datatype

Exception handling is as follows:
- if the field specified by json_path does not exist, return NULL
- if datatype of the field specified by json_path is not the same with type of jsonb_extract_t, return t if it can be cast to t else NULL

#### Syntax

`JSONB jsonb_extract(JSONB j, VARCHAR json_path)`

`BOOLEAN jsonb_extract_isnull(JSONB j, VARCHAR json_path)`

`BOOLEAN jsonb_extract_bool(JSONB j, VARCHAR json_path)`

`INT jsonb_extract_int(JSONB j, VARCHAR json_path)`

`BIGINT jsonb_extract_bigint(JSONB j, VARCHAR json_path)`

`DOUBLE jsonb_extract_double(JSONB j, VARCHAR json_path)`

`STRING jsonb_extract_string(JSONB j, VARCHAR json_path)`


## jsonb_exists_path and jsonb_type
### description

There are two extra functions to check field existence and type
- jsonb_exists_path check the existence of the field specified by json_path, return TRUE or FALS
- jsonb_exists_path get the type as follows of the field specified by json_path, return NULL if it does not exist
  - object
  - array
  - null
  - bool
  - int
  - bigint
  - double
  - string

`BOOLEAN jsonb_exists_path(JSONB j, VARCHAR json_path)`

`STRING jsonb_type(JSONB j, VARCHAR json_path)`


### example

refer to jsonb tutorial for more.


### keywords
JSONB, JSON, jsonb_extract, jsonb_extract_isnull, jsonb_extract_bool, jsonb_extract_int, jsonb_extract_bigint, jsonb_extract_double, jsonb_extract_string, jsonb_exists_path, jsonb_type
