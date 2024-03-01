---
{
    "title": "random_bytes",
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

## random_bytes
### description

random_bytes函数用于生成随机字节序列。

#### Syntax

```sql
VARCHAR random_bytes(INT len)
```

### Parameters

- len: 该参数指定生成的随机字节序列的长度。

### example

```
mysql> select random_bytes(7);
+------------------------------------------------+
| random_bytes(7) |
+------------------------------------------------+
| 0x53edd97401fb6d                               |
+------------------------------------------------+
```

### keywords
    RANDOM BYTES
