---
{
    "title": "hex",
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

## hex

### description
#### Syntax

`STRING hex(BIGINT X)` `STRING hex(STRING x)`
返回十六进制的字符串表示。如果输入是字符串，则字符串中每个字符都转换为两个十六进制数字.

### example

```
mysql> SELECT hex(255);
+----------+
| hex(255) |
+----------+
| FF       |
+----------+
mysql> SELECT hex('abcd');
+-------------+
| hex('abcd') |
+-------------+
| 61626364    |
+-------------+
```

### keywords
	HEX
