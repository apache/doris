---
{
    "title": "REGEXP_REPLACE_ONE",
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

## regexp_replace_one
### description
#### Syntax

`VARCHAR regexp_replace_one(VARCHAR str, VARCHAR pattern, VARCHAR repl)`


Regular matching of STR strings, replacing the part hitting pattern with repl, replacing only the first match.

### example

```
mysql> SELECT regexp_replace_one('a b c', " ", "-");
+-----------------------------------+
| regexp_replace_one('a b c', ' ', '-') |
+-----------------------------------+
| a-b c                             |
+-----------------------------------+

mysql> SELECT regexp_replace_one('a b b','(b)','<\\1>');
+----------------------------------------+
| regexp_replace_one('a b b', '(b)', '<\1>') |
+----------------------------------------+
| a <b> b                                |
+----------------------------------------+
```
### keywords
    REGEXP_REPLACE_ONE,REGEXP,REPLACE,ONE
