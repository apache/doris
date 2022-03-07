---
{
    "title": "compare_version",
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

# compare_version
## Description
### Syntax

`BOOLEAN compare_version (VARCHAR str, VARCHAR str, VARCHAR str)`

返回两个版本的比较结果.

## example

```
mysql> select compare_version('11.1.1.','>=','10.1.1') value;
+-------+
| value |
+-------+
|     1 |
+-------+
mysql> select compare_version('9.1.1.','>=','10.1.1') value;
+-------+
| value |
+-------+
|     0 |
+-------+
mysql> select compare_version('9.1.1.','<=','10.1.1') value;
+-------+
| value |
+-------+
|     1 |
+-------+
mysql> select compare_version('11.1.1.','>=','aa.1.1') value;
+-------+
| value |
+-------+
|     0 |
+-------+
mysql> select compare_version('','>=','') value;
+-------+
| value |
+-------+
|     0 |
+-------+
```
## keyword
COMPARE_VERSION