---
{
    "title": "MICROSECONDS_SUB",
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

## microseconds_sub
### description
#### Syntax

`DATETIMEV2 microseconds_sub(DATETIMEV2 basetime, INT delta)`
- basetime: DATETIMEV2 类型起始时间
- delta: 从 basetime 起需要扣减的微秒数
- 返回类型为 DATETIMEV2

### example
```
mysql> select now(3), microseconds_sub(now(3), 100000);
+-------------------------+----------------------------------+
| now(3)                  | microseconds_sub(now(3), 100000) |
+-------------------------+----------------------------------+
| 2023-02-25 02:03:05.174 | 2023-02-25 02:03:05.074          |
+-------------------------+----------------------------------+
```
`now(3)` 返回精度位数 3 的 DATETIMEV2 类型当前时间，`microseconds_add(now(3), 100000)` 返回当前时间减去 100000 微秒后的 DATETIMEV2 类型时间

### keywords
    microseconds_sub

    