---
{
    "title": "CBRT",
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

## cbrt

### description
#### Syntax

`DOUBLE cbrt(DOUBLE x)`
Returns the cube root of x.

### example

```
mysql> select cbrt(8);
+-----------+
| cbrt(8.0) |
+-----------+
|         2 |
+-----------+
mysql> select cbrt(2.0);
+--------------------+
| cbrt(2.0)          |
+--------------------+
| 1.2599210498948734 |
+--------------------+
mysql> select cbrt(-1000.0);
+---------------+
| cbrt(-1000.0) |
+---------------+
|           -10 |
+---------------+
```

### keywords
	CBRT
