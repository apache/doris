---
{
    "title": "JSON_UNQUOTE",
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

## json_unquote
### Description
#### Syntax

`VARCHAR json_unquote(VARCHAR)`

This function unquotes a JSON value and returns the result as a utf8mb4 string. If the argument is NULL, it will return NULL.

Escape sequences within a string as shown in the following table will be recognized. Backslashes will be ignored for all other escape sequences.

| Escape Sequence | Character Represented by Sequence  |
|-----------------|------------------------------------|
| \"              | A double quote (") character       |
| \b              | A backspace character              |
| \f              | A formfeed character               |
| \n              | A newline (linefeed) character     |
| \r              | A carriage return character        |
| \t              | A tab character                    |
| \\              | A backslash (\) character          |
| \uxxxx          | UTF-8 bytes for Unicode value XXXX |



### example

```
mysql> SELECT json_unquote('"doris"');
+-------------------------+
| json_unquote('"doris"') |
+-------------------------+
| doris                   |
+-------------------------+

mysql> SELECT json_unquote('[1, 2, 3]');
+---------------------------+
| json_unquote('[1, 2, 3]') |
+---------------------------+
| [1, 2, 3]                 |
+---------------------------+


mysql> SELECT json_unquote(null);
+--------------------+
| json_unquote(NULL) |
+--------------------+
| NULL               |
+--------------------+

mysql> SELECT json_unquote('"\\ttest"');
+--------------------------+
| json_unquote('"\ttest"') |
+--------------------------+
|       test                    |
+--------------------------+
```
### keywords
json,unquote,json_unquote
