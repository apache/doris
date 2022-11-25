---
{
    "title": "numbers",
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

## `numbers`

### description

表函数，生成一张只含有一列的临时表，列名为`number`，行的值为[0,n)。

该函数用于from子句中。

语法：

```
numbers(
  "number" = "n",
  "backend_num" = "m"
  );
```

参数：
- `number`: 代表生成[0,n)的行。
- `backend_num`: 可选参数,代表`m`个be节点同时执行该函数（需要部署多个be）。

### example
```
mysql> select * from numbers("number" = "10");
+--------+
| number |
+--------+
|      0 |
|      1 |
|      2 |
|      3 |
|      4 |
|      5 |
|      6 |
|      7 |
|      8 |
|      9 |
+--------+
```

### keywords

    numbers


