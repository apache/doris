---
{
    "title": "seconds_sub",
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

## seconds_sub
### description
#### Syntax

`DATETIME SECONDS_SUB(DATETIME date, INT seconds)`

Subtracts a specified number of seconds from a datetime or date

The parameter date can be DATETIME or DATE, and the return type is DATETIME.

### example

```
mysql> select seconds_sub("2020-01-01 00:00:00", 1);
+---------------------------------------+
| seconds_sub('2020-01-01 00:00:00', 1) |
+---------------------------------------+
| 2019-12-31 23:59:59                   |
+---------------------------------------+
```

### keywords

    SECONDS_SUB
