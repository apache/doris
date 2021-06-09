---
{
    "title": "curdate",
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

# curdate
## description
### Syntax

`DATE CURDATE()`

获取当前的日期，以DATE类型返回。

**注意**：`CURDATE() + 0` 返回格式为`yyyyMMddHHmmss`的日期时间数值。

## Examples

```
mysql> select CURDATE();
+------------+
| curdate()  |
+------------+
| 2021-06-09 |
+------------+

mysql> select CURDATE() + 0;
+-----------------+
| curdate() + 0.0 |
+-----------------+
|  20210609144151 |
+-----------------+
````

## keyword

    CURDATE
