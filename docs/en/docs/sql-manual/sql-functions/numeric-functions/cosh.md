---
{
    "title": "COSH",
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

## cosh

### description
#### Syntax

`DOUBLE cosh(DOUBLE x)`
Returns the hyperbolic cosine of `x`.

### example

```
mysql> select cosh(0);
+---------+
| cosh(0) |
+---------+
|       1 |
+---------+

mysql> select cosh(1);
+---------------------+
| cosh(1)             |
+---------------------+
| 1.5430806348152437  |
+---------------------+
```

### keywords
	COSH
