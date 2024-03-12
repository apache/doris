---
{
    "title": "HLL_TO_BASE64",
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

## hll_to_base64

### description
#### Syntax

`VARCHAR HLL_TO_BASE64(HLL input)`

Convert an input hll to a base64 string. If input is NULL, return NULL.

### example

```
mysql> select hll_to_base64(NULL);
+---------------------+
| hll_to_base64(NULL) |
+---------------------+
| NULL                |
+---------------------+
1 row in set (0.00 sec)

mysql> select hll_to_base64(hll_empty());
+----------------------------+
| hll_to_base64(hll_empty()) |
+----------------------------+
| AA==                       |
+----------------------------+
1 row in set (0.02 sec)

mysql> select hll_to_base64(hll_hash('abc'));
+--------------------------------+
| hll_to_base64(hll_hash('abc')) |
+--------------------------------+
| AQEC5XSzrpDsdw==               |
+--------------------------------+
1 row in set (0.03 sec)

mysql> select hll_union_agg(hll_from_base64(hll_to_base64(pv))), hll_union_agg(pv) from test_hll;
+---------------------------------------------------+-------------------+
| hll_union_agg(hll_from_base64(hll_to_base64(pv))) | hll_union_agg(pv) |
+---------------------------------------------------+-------------------+
|                                                 3 |                 3 |
+---------------------------------------------------+-------------------+
1 row in set (0.04 sec)

mysql>  select hll_cardinality(hll_from_base64(hll_to_base64(hll_hash('abc'))));
+------------------------------------------------------------------+
| hll_cardinality(hll_from_base64(hll_to_base64(hll_hash('abc')))) |
+------------------------------------------------------------------+
|                                                                1 |
+------------------------------------------------------------------+
1 row in set (0.04 sec)
```

### keywords

    HLL_TO_BASE64,HLL
