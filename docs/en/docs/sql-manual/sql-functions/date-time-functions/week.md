---
{
    "title": "WEEK",
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

## week
### Description
#### Syntax

`INT WEEK(DATE date[, INT mode])`

Returns the week number for date.The value of the mode argument defaults to 0.
The following table describes how the mode argument works.

|Mode |First day of week |Range  |Week 1 is the first week …    |
|:----|:-----------------|:------|:-----------------------------|
|0    |Sunday            |0-53   |with a Sunday in this year    |
|1    |Monday            |0-53   |with 4 or more days this year |
|2    |Sunday            |1-53   |with a Sunday in this year    |
|3    |Monday            |1-53   |with 4 or more days this year |
|4    |Sunday            |0-53   |with 4 or more days this year |
|5    |Monday            |0-53   |with a Monday in this year    |
|6    |Sunday            |1-53   |with 4 or more days this year |
|7    |Monday            |1-53   |with a Monday in this year    |

The parameter is Date or Datetime type

### example
```
mysql> select week('2020-1-1');
+------------------+
| week('2020-1-1') |
+------------------+
|                0 |
+------------------+
```
```
mysql> select week('2020-7-1',1);
+---------------------+
| week('2020-7-1', 1) |
+---------------------+
|                  27 |
+---------------------+
```
### keywords
    WEEK
