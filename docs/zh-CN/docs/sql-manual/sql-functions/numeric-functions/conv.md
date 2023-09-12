---
{
    "title": "CONV",
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

## conv

### description
#### Syntax

```sql
VARCHAR CONV(VARCHAR input, TINYINT from_base, TINYINT to_base)
VARCHAR CONV(BIGINT input, TINYINT from_base, TINYINT to_base)
```
对输入的数字进行进制转换，输入的进制范围应该在`[2,36]`以内。

### example

```
MySQL [test]> SELECT CONV(15,10,2);
+-----------------+
| conv(15, 10, 2) |
+-----------------+
| 1111            |
+-----------------+

MySQL [test]> SELECT CONV('ff',16,10);
+--------------------+
| conv('ff', 16, 10) |
+--------------------+
| 255                |
+--------------------+

MySQL [test]> SELECT CONV(230,10,16);
+-------------------+
| conv(230, 10, 16) |
+-------------------+
| E6                |
+-------------------+
```

### keywords
	CONV
