---
{
    "title": "roundBankers",
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

## roundBankers

### description
#### Syntax

`roundBankers(x), roundBankers(x, d)`
Rounds the argument `x` to `d` specified decimal places. `d` defaults to 0 if not specified. If d is negative, the left d digits of the decimal point are 0. If x or d is null, null is returned.

+ If the rounding number is halfway between two numbers, the function uses banker’s rounding.
+ In other cases, the function rounds numbers to the nearest integer.



### example

```
mysql> select roundBankers(0.4);
+-------------------+
| roundBankers(0.4) |
+-------------------+
|                 0 |
+-------------------+
mysql> select roundBankers(-3.5);
+--------------------+
| roundBankers(-3.5) |
+--------------------+
|                 -4 |
+--------------------+
mysql> select roundBankers(-3.4);
+--------------------+
| roundBankers(-3.4) |
+--------------------+
|                 -3 |
+--------------------+
mysql> select roundBankers(10.755, 2);
+-------------------------+
| roundBankers(10.755, 2) |
+-------------------------+
|                   10.76 |
+-------------------------+
mysql> select roundBankers(1667.2725, 2);
+----------------------------+
| roundBankers(1667.2725, 2) |
+----------------------------+
|                    1667.27 |
+----------------------------+
mysql> select roundBankers(1667.2725, -2);
+-----------------------------+
| roundBankers(1667.2725, -2) |
+-----------------------------+
|                        1700 |
+-----------------------------+
```

### keywords
	ROUNDBANKERS
