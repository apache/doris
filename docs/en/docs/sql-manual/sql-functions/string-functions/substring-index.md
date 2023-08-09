---
{
"title": "SUBSTRING_INDEX",
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

## substring_index

### Name

<version since="1.2">

SUBSTRING_INDEX

</version>

### description

#### Syntax

`VARCHAR substring_index(VARCHAR content, VARCHAR delimiter, INT field)`

Split `content` to two parts at position where the `field`s of `delimiter` stays, return one of them according to below rules:
if `field` is positive, return the left part;
else if `field` is negative, return the right part;
if `field` is zero, return an empty string when `content` is not null, else will return null.

- `delimiter` is case sensitive and multi-byte safe.
- `delimiter` and `field` parameter should be constant.


### example

```
mysql> select substring_index("hello world", " ", 1);
+----------------------------------------+
| substring_index("hello world", " ", 1) |
+----------------------------------------+
| hello                                  |
+----------------------------------------+
mysql> select substring_index("hello world", " ", 2);
+----------------------------------------+
| substring_index("hello world", " ", 2) |
+----------------------------------------+
| hello world                            |
+----------------------------------------+
mysql> select substring_index("hello world", " ", -1);
+-----------------------------------------+
| substring_index("hello world", " ", -1) |
+-----------------------------------------+
| world                                   |
+-----------------------------------------+
mysql> select substring_index("hello world", " ", -2);
+-----------------------------------------+
| substring_index("hello world", " ", -2) |
+-----------------------------------------+
| hello world                             |
+-----------------------------------------+
mysql> select substring_index("hello world", " ", -3);
+-----------------------------------------+
| substring_index("hello world", " ", -3) |
+-----------------------------------------+
| hello world                             |
+-----------------------------------------+
mysql> select substring_index("hello world", " ", 0);
+----------------------------------------+
| substring_index("hello world", " ", 0) |
+----------------------------------------+
|                                        |
+----------------------------------------+
```
### keywords

    SUBSTRING_INDEX, SUBSTRING