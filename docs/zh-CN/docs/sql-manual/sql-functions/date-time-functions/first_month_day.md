---
{
"title": "first_month_day",
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

## first_month_day
### Description
#### Syntax

`DATE first_month_day(DATETIME date)`

返回输入日期中月份的第一天；所以返回的日期中，年和月不变，日置为01

### example

```
mysql > select first_month_day('2021-12-22');
+-------------------+
| first_month_day('2021-12-22 00:00:00') |
+-------------------+
| 2021-12-01        |
+-------------------+
```

### keywords
    FIRST_MONTH_DAY,MONTH_DAY,DAYS

