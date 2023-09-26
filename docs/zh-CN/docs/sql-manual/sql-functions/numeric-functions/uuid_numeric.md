---
{
    "title": "uuid_numeric",
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

## uuid_numeric
### description
#### Syntax

`LARGEINT uuid_numeric()`

返回一个 `LARGEINT` 类型的 `uuid`。注意 `LARGEINT` 是一个 Int128，所以 `uuid_numeric()` 可能会得到负值。

### example

```

mysql> select uuid_numeric();
+----------------------------------------+
| uuid_numeric()                         |
+----------------------------------------+
| 82218484683747862468445277894131281464 |
+----------------------------------------+
```

### keywords
    
    UUID UUID-NUMERIC 
