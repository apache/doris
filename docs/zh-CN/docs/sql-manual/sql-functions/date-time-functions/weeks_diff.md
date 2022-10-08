---
{
    "title": "weeks_diff",
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

## weeks_diff
### description
#### Syntax

`INT weeks_diff(DATETIME enddate, DATETIME startdate)`

开始时间到结束时间相差几星期

### example

```
mysql> select weeks_diff('2020-12-25','2020-10-25');
+----------------------------------------------------------+
| weeks_diff('2020-12-25 00:00:00', '2020-10-25 00:00:00') |
+----------------------------------------------------------+
|                                                        8 |
+----------------------------------------------------------+
```

### keywords

    weeks_diff
