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

# substr
## description
### Syntax

`VARCHAR substr(VARCHAR str, start position, count)`


它返回从start位置开始计算，count个字符串

## example

```
mysql> select substr("Hello doris",5,3);
+-----------------------------+
| substr('Hello doris', 5, 3) |
+-----------------------------+
| o d                         |
+-----------------------------+
```
##keyword
substr
