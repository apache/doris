---
{
    "title": "NOW",
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

## now
### description
#### Syntax

`DATETIME NOW()`


获得当前的时间，以Datetime类型返回

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


获得当前的时间，以DatetimeV2类型返回
precision代表了用户想要的秒精度，当前精度最多支持到微秒，即precision取值范围为[0, 6]。

### example

```
mysql> select now(3);
+-------------------------+
| now(3)                  |
+-------------------------+
| 2022-09-06 16:13:30.078 |
+-------------------------+
```

注意：
1. 当前只有DatetimeV2数据类型可支持秒精度
2. 受限于JDK实现，如果用户使用JDK8构建FE，则精度最多支持到毫秒（小数点后三位），更大的精度位将全部填充0。如果用户有更高精度需求，请使用JDK11。

### keywords

    NOW
