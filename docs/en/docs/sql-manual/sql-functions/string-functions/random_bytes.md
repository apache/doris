---
{
    "title": "random_bytes",
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

## random_bytes
### description

The `random_bytes` function generates a sequence of random bytes.

#### Syntax

```sql
VARCHAR random_bytes(INT len)
```

### Parameters

- len: The `random_bytes` function takes a single argument, which specifies the length of the generated random byte sequence.

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
