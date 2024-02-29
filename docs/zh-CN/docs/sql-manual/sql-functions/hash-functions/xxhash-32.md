---
{
    "title": "XXHASH_32",
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

## xxhash_32

### description
#### Syntax

`INT XXHASH_32(VARCHAR input, ...)`

返回输入字符串的32位xxhash值。

注：在计算hash值时，更推荐使用`xxhash_32`，而不是`murmur_hash3_32`。

### example

```
mysql> select xxhash_32(NULL);
+-----------------+
| xxhash_32(NULL) |
+-----------------+
|            NULL |
+-----------------+

mysql> select xxhash_32("hello");
+--------------------+
| xxhash_32('hello') |
+--------------------+
|          -83855367 |
+--------------------+

mysql> select xxhash_32("hello", "world");
+-----------------------------+
| xxhash_32('hello', 'world') |
+-----------------------------+
|                  -920844969 |
+-----------------------------+
```

### keywords
HASH_32,HASH
