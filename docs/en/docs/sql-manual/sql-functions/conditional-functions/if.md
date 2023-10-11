---
{
    "title": "IF",
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

## if
### description
#### Syntax

`if(boolean condition, type valueTrue, type valueFalseOrNull)`


Returns valueTrue when condition is true, returns valueFalseOrNull otherwise. 

The return type is the type of the result of the valueTrue/valueFalseOrNull expression  
:::tip
The if function has three parameters, and its calculation process is as follows:
1. First calculate the results of the second parameter and the third parameter together

2. Then, depending on the condition of the first parameter, select the value of the second or third

Instead of choosing to execute the second or third parameter based on the condition of the first parameter, this needs to be paid attention to.
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
