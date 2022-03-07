---
{
    "title": "coalesce",
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

# coalesce
## Description
### Syntax

`VARCHAR coalesce(VARCHAR, ...)`
`...`
`INT coalesce(INT, ...)`

` coalesce ` function will return the first not null value. If it's all value is null, return null

## example

```
MySQL> select coalesce(1,null,2);
+----------------------+
| coalesce(1, NULL, 2) |
+----------------------+
|                    1 |
+----------------------+

MySQL> select coalesce(null,"asd",1);
+--------------------------+
| coalesce(NULL, 'asd', 1) |
+--------------------------+
| asd                      |
+--------------------------+

MySQL> select coalesce(null,null,null);
+----------------------------+
| coalesce(NULL, NULL, NULL) |
+----------------------------+
|                       NULL |
+----------------------------+
```
## keyword
coalesce
