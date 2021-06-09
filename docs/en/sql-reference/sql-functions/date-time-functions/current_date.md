---
{
    "title": "current_date",
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

# current_date
## Description
### Syntax

'DATE CURRENT_DATE()'

Get the current date and return it in Date type.

## example

```
mysql> select CURRENT_DATE();
+----------------+
| CURRENT_DATE() |
+----------------+
| 2021-06-09     |
+----------------+

mysql> select CURRENT_DATE() + 0;
+--------------------+
| CURRENT_DATE() + 0 |
+--------------------+
|           20210609 |
+--------------------+
```

## keyword
CURRENT_DATE