---
{
    "title": "JSON_UNQUOTE",
    "language": "zh-CN"
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

这个函数将去掉JSON值中的引号，并将结果作为utf8mb4字符串返回。如果参数为NULL，则返回NULL。

在字符串中显示的如下转义序列将被识别，对于所有其他转义序列，反斜杠将被忽略。

| 转义序列 | 序列表示的字符                |
|----------|-------------------------------|
| \"       | 双引号 "                      |
| \b       | 退格字符                      |
| \f       | 换页符                        |
| \n       | 换行符                        |
| \r       | 回车符                        |
| \t       | 制表符                        |
| \\       | 反斜杠 \                      |
| \uxxxx   | Unicode 值 XXXX 的 UTF-8 字节 |



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
