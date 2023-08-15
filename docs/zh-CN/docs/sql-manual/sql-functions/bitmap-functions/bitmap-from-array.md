---
{
    "title": "BITMAP_FROM_ARRAY",
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

## bitmap_from_array

### description
#### Syntax

`BITMAP BITMAP_FROM_ARRAY(ARRAY input)`

将一个TINYINT/SMALLINT/INT/BIGINT类型的数组转化为一个BITMAP
当输入字段不合法时，结果返回NULL

### example

```
mysql> select *, bitmap_to_string(bitmap_from_array(c_array)) from array_test;
+------+-----------------------+------------------------------------------------+
| id   | c_array               | bitmap_to_string(bitmap_from_array(`c_array`)) |
+------+-----------------------+------------------------------------------------+
|    1 | [NULL]                | NULL                                           |
|    2 | [1, 2, 3, NULL]       | NULL                                           |
|    2 | [1, 2, 3, -10]        | NULL                                           |
|    3 | [1, 2, 3, 4, 5, 6, 7] | 1,2,3,4,5,6,7                                  |
|    4 | [100, 200, 300, 300]  | 100,200,300                                    |
+------+-----------------------+------------------------------------------------+
5 rows in set (0.02 sec)
```

### keywords

    BITMAP_FROM_ARRAY,BITMAP
