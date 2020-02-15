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
## Description
### Syntax

`BOOLEAN NOT_EMPTY (VARCHAR str)`

It returns true if the string is a non-empty string, otherwise it returns false.
If any parameter is NULL, it returns NULL.



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