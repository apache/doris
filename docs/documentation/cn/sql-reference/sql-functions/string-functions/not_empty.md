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

# not_empty
## description
### Syntax

`BOOLEAN NOT_EMPTY (VARCHAR str)`

如果字符串为非空字符串，返回true。否则，返回false。任意参数为NULL，返回NULL。

## example

```
MySQL [(none)]> select not_empty(" ");
+----------------+
| not_empty(' ') |
+----------------+
|              1 |
+----------------+

MySQL [(none)]> select not_empty("");
+---------------+
| not_empty('') |
+---------------+
|             0 |
+---------------+
```
##keyword
NOT_EMPTY