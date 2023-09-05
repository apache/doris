---
{
    "title": "UNHEX_TO_BITMAP",
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

## unhex_to_bitmap
### description
#### Syntax

`BITMAP UNHEX_TO_BITMAP(expr)`

使用场景：从doris查出bitmap的二进制数据，经过计算后，可以再把二进制数据导回doris。

输入为bitmap二进制数据的十六进制格式字符串，前两位表示bitmap类型：0为空；1为单个元素；2为多个元素。后面八位为采用小端格式的数据内容。
输出为bitmap对象。
当输入值不在此范围时，会返回NULL。

### example

```
mysql> select bitmap_to_string(unhex_to_bitmap('0106000000'));
+-------------------------------------------------+
| bitmap_to_string(unhex_to_bitmap('0106000000')) |
+-------------------------------------------------+
| 6                                               |
+-------------------------------------------------+
```

### keywords

    UNHEX_TO_BITMAP,BITMAP
