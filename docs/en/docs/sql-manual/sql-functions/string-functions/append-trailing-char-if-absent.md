---
{
    "title": "APPEND_TRAILING_CHAR_IF_ABSENT",
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

## append_trailing_char_if_absent

### description

#### Syntax

`VARCHAR append_trailing_char_if_absent(VARCHAR str, VARCHAR trailing_char)`

If the @str string is non-empty and does not contain the @trailing_char character at the end, it appends the @trailing_char character to the end.
@trailing_char contains only one character, and it will return NULL if contains more than one character

### example

```
MySQL [test]> select append_trailing_char_if_absent('a','c');
+------------------------------------------+
| append_trailing_char_if_absent('a', 'c') |
+------------------------------------------+
| ac                                       |
+------------------------------------------+
1 row in set (0.02 sec)

MySQL [test]> select append_trailing_char_if_absent('ac','c');
+-------------------------------------------+
| append_trailing_char_if_absent('ac', 'c') |
+-------------------------------------------+
| ac                                        |
+-------------------------------------------+
1 row in set (0.00 sec)
```

### keywords

    APPEND_TRAILING_CHAR_IF_ABSENT
