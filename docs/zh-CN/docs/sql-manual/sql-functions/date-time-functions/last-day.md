---
{
    "title": "LAST_DAY",
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

## last_day
### Description
#### Syntax

`DATE last_day(DATETIME date)`

返回输入日期中月份的最后一天；所以返回的日期中，年和月不变，日可能是如下情况：
'28'(非闰年的二月份), 
'29'(闰年的二月份),
'30'(四月，六月，九月，十一月),
'31'(一月，三月，五月，七月，八月，十月，十二月)

### example

```
mysql > select last_day('2000-02-03');
+-------------------+
| last_day('2000-02-03 00:00:00') |
+-------------------+
| 2000-02-29        |
+-------------------+
```

### keywords
    LAST_DAY,DAYS
