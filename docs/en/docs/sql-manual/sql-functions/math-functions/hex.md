---
{
    "title": "hex",
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

## hex

### description
#### Syntax

`STRING hex(BIGINT X)` `STRING hex(STRING x)`
Returns the string representation in hexadecimal. If the input is a string, each character in the string is converted to two hexadecimal digits.

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
