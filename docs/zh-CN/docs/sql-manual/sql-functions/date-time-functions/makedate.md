---
{
    "title": "MAKEDATE",
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

## makedate
### description
#### Syntax

`DATE MAKEDATE(INT year, INT dayofyear)`

返回指定年份和dayofyear构建的日期。dayofyear必须大于0，否则结果为空。

### example
```
mysql> select makedate(2021,1), makedate(2021,100), makedate(2021,400);
+-------------------+---------------------+---------------------+
| makedate(2021, 1) | makedate(2021, 100) | makedate(2021, 400) |
+-------------------+---------------------+---------------------+
| 2021-01-01        | 2021-04-10          | 2022-02-04          |
+-------------------+---------------------+---------------------+
```

### keywords

    MAKEDATE
