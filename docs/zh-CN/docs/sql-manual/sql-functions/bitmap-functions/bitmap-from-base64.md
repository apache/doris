---
{
    "title": "BITMAP_FROM_BASE64",
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

## bitmap_from_base64

### description
#### Syntax

`BITMAP BITMAP_FROM_BASE64(VARCHAR input)`

将一个base64字符串(`bitmap_to_base64`函数的结果)转化为一个BITMAP。当输入字符串不合法时，返回NULL。

### example

```
mysql> select bitmap_to_string(bitmap_from_base64("AA=="));
+----------------------------------------------+
| bitmap_to_string(bitmap_from_base64("AA==")) |
+----------------------------------------------+
|                                              |
+----------------------------------------------+

mysql> select bitmap_to_string(bitmap_from_base64("AQEAAAA="));
+-----------------------------------+
| bitmap_to_string(bitmap_from_base64("AQEAAAA=")) |
+-----------------------------------+
| 1                                 |
+-----------------------------------+

mysql> select bitmap_to_string(bitmap_from_base64("AjowAAACAAAAAAAAAJgAAAAYAAAAGgAAAAEAf5Y="));
+----------------------------------------------------------------------------------+
| bitmap_to_string(bitmap_from_base64("AjowAAACAAAAAAAAAJgAAAAYAAAAGgAAAAEAf5Y=")) |
+----------------------------------------------------------------------------------+
| 1,9999999                                                                        |
+----------------------------------------------------------------------------------+
```

### keywords

    BITMAP_FROM_BASE64,BITMAP
