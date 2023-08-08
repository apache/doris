---
{
    "title": "NOW",
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

## now
### Description
#### Syntax

`DATETIME NOW ()`


Get the current time and return it in Datetime type.

### example

```
mysql> select now();
+---------------------+
| now()               |
+---------------------+
| 2019-05-27 15:58:25 |
+---------------------+
```

`DATETIMEV2 NOW(INT precision)`


Get the current time and return it in DatetimeV2 type.
Precision represents the second precision that the user wants. The current precision supports up to microseconds, that is, the value range of precision is [0, 6].

### example

```
mysql> select now(3);
+-------------------------+
| now(3)                  |
+-------------------------+
| 2022-09-06 16:13:30.078 |
+-------------------------+
```

Note:
1. Currently, only DatetimeV2 type supports precision.
2. Limited by the JDK implementation, if you use jdk8 to build FE, the precision can be up to milliseconds (three decimal places), and the larger precision bits will be filled with 0. If you need higher accuracy, please use jdk11 to build FE.

### keywords
    NOW
