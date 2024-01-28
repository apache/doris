---
{
"title": "BIT_COUNT",
"language": "zh-CH"
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

## bit_count
### description
#### Syntax

`BIT_COUNT(Integer-type x)`

统计整型 x 的二的补码表示中 1 的个数。

整型可以是：TINYINT、SMALLINT、INT、BIGINT、LARGEINT

### example

```
select "0b11111111", bit_count(-1)
--------------

+--------------+---------------+
| '0b11111111' | bit_count(-1) |
+--------------+---------------+
| 0b11111111   |             8 |
+--------------+---------------+
```

### keywords

    BITCOUNT, BIT_COUNT
