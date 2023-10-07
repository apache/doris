---
{
"title": "SHA2",
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

## SHA2

### description

Use SHA2 to digest the message.

#### Syntax

`SHA2(str, digest_length)`

#### Arguments

- `str`: content to be encrypted
- `digest_length`: the length of the digest

### example

```SQL
mysql> select sha2('abc', 224);
+----------------------------------------------------------+
| sha2('abc', 224)                                         |
+----------------------------------------------------------+
| 23097d223405d8228642a477bda255b32aadbce4bda0b3f7e36c9da7 |
+----------------------------------------------------------+
1 row in set (0.13 sec)

mysql> select sha2('abc', 384);
+--------------------------------------------------------------------------------------------------+
| sha2('abc', 384)                                                                                 |
+--------------------------------------------------------------------------------------------------+
| cb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed8086072ba1e7cc2358baeca134c825a7 |
+--------------------------------------------------------------------------------------------------+
1 row in set (0.13 sec)

mysql> select sha2(NULL, 512);
+-----------------+
| sha2(NULL, 512) |
+-----------------+
| NULL            |
+-----------------+
1 row in set (0.09 sec)
```

### keywords

    SHA2
