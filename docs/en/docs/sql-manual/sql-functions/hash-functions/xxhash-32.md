---
{
    "title": "XXHASH_32",
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

## xxhash_32

### description
#### Syntax

`INT XXHASH_32(VARCHAR input, ...)`

Return the 32 bits xxhash of input string.

Note: When calculating hash values, it is more recommended to use `xxhash_32` instead of `murmur_hash3_32`.

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

XXHASH_32,HASH
