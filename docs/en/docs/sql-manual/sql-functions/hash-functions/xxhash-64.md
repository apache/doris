---
{
    "title": "XXHASH_64",
    "language": "en"
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

## xxhash_64

### description
#### Syntax

`BIGINT XXHASH_64(VARCHAR input, ...)`

Return the 64 bits xxhash of input string.

Note: When calculating hash values, it is more recommended to use `xxhash_64` instead of `murmur_hash3_64`.

### example

```
mysql> select xxhash_64(NULL);
+-----------------+
| xxhash_64(NULL) |
+-----------------+
|            NULL |
+-----------------+

mysql> select xxhash_64("hello");
+----------------------+
| xxhash_64('hello')   |
+----------------------+
| -7685981735718036227 |
+----------------------+

mysql> select xxhash_64("hello", "world");
+-----------------------------+
| xxhash_64('hello', 'world') |
+-----------------------------+
|         7001965798170371843 |
+-----------------------------+
```

### benchmark

Through TPCH Benchmark testing, it was found that `xxhash_64` has significantly improved performance compared to `murmur_hash3_64`. Therefore, in scenarios where hash values need to be calculated, it is more recommended to use `xxhash_64`.

```
mysql> select count(murmur_hash3_64(l_comment)) from lineitem;
+-----------------------------------+
| count(murmur_hash3_64(l_comment)) |
+-----------------------------------+
|                         600037902 |
+-----------------------------------+
1 row in set (17.18 sec)

mysql> select count(xxhash_64(l_comment)) from lineitem;
+-----------------------------+
| count(xxhash_64(l_comment)) |
+-----------------------------+
|                   600037902 |
+-----------------------------+
1 row in set (8.41 sec)
```

### keywords

XXHASH_64,HASH
