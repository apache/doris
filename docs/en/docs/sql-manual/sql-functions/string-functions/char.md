---
{
    "title": "CHAR",
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

<version since="2.0">

## function char
### description
#### Syntax

`VARCHAR char(INT,..., [USING charset_name])`

Interprets each argument as an integer and returns a string consisting of the characters given by the code values of those integers. `NULL` values are skipped.

If the result string is illegal for the given character set, the result from `CHAR()` becomes `NULL`.

Arguments larger than `255` are converted into multiple result bytes. For example, `char(15049882)` is equivalent to `char(229, 164, 154)`.

Currently only `utf8` is supported for `charset_name`.
</version>

### example

```
mysql> select char(68, 111, 114, 105, 115);
+--------------------------------------+
| char('utf8', 68, 111, 114, 105, 115) |
+--------------------------------------+
| Doris                                |
+--------------------------------------+

mysql> select char(15049882, 15179199, 14989469);
+--------------------------------------------+
| char('utf8', 15049882, 15179199, 14989469) |
+--------------------------------------------+
| 多睿丝                                     |
+--------------------------------------------+

mysql> select char(255);
+-------------------+
| char('utf8', 255) |
+-------------------+
| NULL              |
+-------------------+
```
### keywords
    CHAR
