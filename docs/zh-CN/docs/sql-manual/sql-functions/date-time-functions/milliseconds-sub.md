---
{
    "title": "MILLISECONDS_SUB",
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

## milliseconds_sub
### description
#### Syntax

`DATETIMEV2 milliseconds_sub(DATETIMEV2 basetime, INT delta)`
- basetime: DATETIMEV2 类型起始时间
- delta: 从 basetime 起需要扣减的毫秒数
- 返回类型为 DATETIMEV2

### example
```
mysql> select milliseconds_sub('2023-09-08 16:02:08.435123', 1);
+--------------------------------------------------------------------------+
| milliseconds_sub(cast('2023-09-08 16:02:08.435123' as DATETIMEV2(6)), 1) |
+--------------------------------------------------------------------------+
| 2023-09-08 16:02:08.434123                                               |
+--------------------------------------------------------------------------+
1 row in set (0.11 sec)
```


### keywords
    milliseconds_sub

    