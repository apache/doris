---
{
    "title": "BITMAP_TO_ARRAY",
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

## bitmap_to_array

### description
#### Syntax

`ARRAY_BIGINT bitmap_to_array(BITMAP input)`

Convert a input BITMAP to Array.
If input is null, return null.

### example

```
mysql> select bitmap_to_array(null);
+------------------------+
| bitmap_to_array(NULL)  |
+------------------------+
| NULL                   |
+------------------------+

mysql> select bitmap_to_array(bitmap_empty());
+---------------------------------+
| bitmap_to_array(bitmap_empty()) |
+---------------------------------+
| []                              |
+---------------------------------+

mysql> select bitmap_to_array(to_bitmap(1));
+-------------------------------+
| bitmap_to_array(to_bitmap(1)) |
+-------------------------------+
| [1]                           |
+-------------------------------+

mysql> select bitmap_to_array(bitmap_from_string('1,2,3,4,5'));
+--------------------------------------------------+
| bitmap_to_array(bitmap_from_string('1,2,3,4,5')) |
+--------------------------------------------------+
| [1, 2, 3, 4, 5]                                  |
+--------------------------------------------------

```

### keywords

    BITMAP_TO_ARRAY,BITMAP
