---
{
    "title": "IF",
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

## if
### description
#### Syntax

`if(boolean condition, type valueTrue, type valueFalseOrNull)`


如果表达式 condition 成立，返回结果 valueTrue；否则，返回结果 valueFalseOrNull
返回类型： valueTrue 表达式结果的类型  
:::tip
if函数有三个参数，其计算过程如下：
1. 首先将第二个参数和第三个参数的结果一起计算出来

2. 然后根据第一个参数的条件，选择第二个或第三个的值

而不是先根据第一个参数的条件，选择执行第二个或者第三个参数，这点需要注意。
:::


### example

```
mysql> select  user_id, if(user_id = 1, "true", "false") test_if from test;
+---------+---------+
| user_id | test_if |
+---------+---------+
| 1       | true    |
| 2       | false   |
+---------+---------+
```
### keywords
IF
