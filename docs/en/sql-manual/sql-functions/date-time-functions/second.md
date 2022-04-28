---
{
    "title": "second",
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

# second
## description
### Syntax

`INT SECOND(DATETIME date)`

Returns second information in the time type, ranging from 0,59

The parameter is Date or Datetime type

## example

```
mysql> select second('2018-12-31 23:59:59');
+-----------------------------+
| second('2018-12-31 23:59:59') |
+-----------------------------+
|                          59 |
+-----------------------------+
```
## keyword
SECOND
