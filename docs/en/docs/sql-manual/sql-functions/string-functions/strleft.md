---
{
    "title": "STRLEFT",
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

## strleft
### Description
#### Syntax

`VARCHAR STRLEFT (VARCHAR str, INT len)`


It returns the left part of a string of specified length, length is char length not the byte size. Another alias for this function is `left`.
If the function parameters contain a NULL value, the function will always return NULL. If the integer parameter is less than or equal to 0, it will return an empty value.

### example

```
mysql> select strleft("Hello doris",5);
+------------------------+
| strleft('Hello doris', 5) |
+------------------------+
| Hello                  |
+------------------------+
mysql> select strleft("Hello doris",-5);
+----------------------------+
| strleft('Hello doris', -5) |
+----------------------------+
|                            |
+----------------------------+
mysql> select strleft("Hello doris",NULL);
+------------------------------+
| strleft('Hello doris', NULL) |
+------------------------------+
| NULL                         |
+------------------------------+
mysql> select strleft(NULL,3);
+------------------+
| strleft(NULL, 3) |
+------------------+
| NULL             |
+------------------+
```
### keywords
    STRLEFT, LEFT