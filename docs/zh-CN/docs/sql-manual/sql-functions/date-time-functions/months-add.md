---
{
    "title": "MONTHS_ADD",
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

## months_add
### description
#### Syntax

`DATETIME MONTHS_ADD(DATETIME date, INT months)`

从日期加上指定月份

参数 date 可以是 DATETIME 或者 DATE 类型，返回类型与参数 date 的类型一致。

### example

```
mysql> select months_add("2020-01-31 02:02:02", 1);
+--------------------------------------+
| months_add('2020-01-31 02:02:02', 1) |
+--------------------------------------+
| 2020-02-29 02:02:02                  |
+--------------------------------------+
```

### keywords

    MONTHS_ADD
