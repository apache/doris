---
{
    "title": "BITMAP_FROM_STRING",
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

## bitmap_from_string

### description
#### Syntax

`BITMAP BITMAP_FROM_STRING(VARCHAR input)`

将一个字符串转化为一个BITMAP，字符串是由逗号分隔的一组unsigned bigint数字组成.(数字取值在:0 ~ 18446744073709551615)
比如"0, 1, 2"字符串会转化为一个Bitmap，其中的第0, 1, 2位被设置.
当输入字段不合法时，返回NULL

### example

```
mysql> select bitmap_to_string(bitmap_from_string("0, 1, 2"));
+-------------------------------------------------+
| bitmap_to_string(bitmap_from_string('0, 1, 2')) |
+-------------------------------------------------+
| 0,1,2                                           |
+-------------------------------------------------+

mysql> select bitmap_from_string("-1, 0, 1, 2");
+-----------------------------------+
| bitmap_from_string('-1, 0, 1, 2') |
+-----------------------------------+
| NULL                              |
+-----------------------------------+

mysql> select bitmap_to_string(bitmap_from_string("0, 1, 18446744073709551615"));
+--------------------------------------------------------------------+
| bitmap_to_string(bitmap_from_string('0, 1, 18446744073709551615')) |
+--------------------------------------------------------------------+
| 0,1,18446744073709551615                                           |
+--------------------------------------------------------------------+
```

### keywords

    BITMAP_FROM_STRING,BITMAP
