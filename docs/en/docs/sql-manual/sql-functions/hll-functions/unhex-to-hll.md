---
{
    "title": "UNHEX_TO_HLL",
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

## UNHEX_TO_HLL
### description
#### Syntax

`UNHEX_TO_HLL(value)`

Converts a value to type hll. It is commonly used to import values of common types into hll columns when importing data. 
Usage scenario: The binary data of hll can be detected from doris, and the binary data can be transferred back to doris after calculation. 
 
The input is a hexadecimal string of hll binary data. The first two digits indicate the hll type: 0 is empty; 1 is a single element; 2 is multiple elements. The last 16 bits are the data content in small-endian format. 
Output is an hll object. 
When the input value is not in this range, NULL is returned.

### example
```
mysql> select HLL_CARDINALITY(unhex_to_hll('01010600000000000000'));
+-------------------------------------------------------+
| hll_cardinality(unhex_to_hll('01010600000000000000')) |
+-------------------------------------------------------+
|                                                     1 |
+-------------------------------------------------------+
```
### keywords
HLL,UNHEX_TO_HLL
