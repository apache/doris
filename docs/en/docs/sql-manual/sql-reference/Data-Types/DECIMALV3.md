---
{
    "title": "DECIMALV3",
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

## DECIMALV3

<version since="1.2.1">

DECIMALV3

</version>

### Description
DECIMALV3 (M [,D])

High-precision fixed-point number, M represents the total number of significant digits, and D represents the scale.

The range of M is [1, 38], and the range of D is [0, precision].

The default value is DECIMALV3(9, 0).

### Precision Deduction

DECIMALV3 has a very complex set of type inference rules. For different expressions, different rules will be applied for precision inference.

#### Arithmetic Expressions

* Plus / Minus: DECIMALV3(a, b) + DECIMALV3(x, y) -> DECIMALV3(max(a - b, x - y) + max(b, y), max(b, y)). That is, the integer part and the decimal part use the larger value of the two operands respectively.
* Multiply: DECIMALV3(a, b) + DECIMALV3(x, y) -> DECIMALV3(a + x, b + y).
* Divide: DECIMALV3(a, b) + DECIMALV3(x, y) -> DECIMALV3(a + y, b).

#### Aggregation functions

* SUM / MULTI_DISTINCT_SUM: SUM(DECIMALV3(a, b)) -> DECIMALV3(38, b).
* AVG: AVG(DECIMALV3(a, b)) -> DECIMALV3(38, max(b, 4)).

#### Default rules

Except for the expressions mentioned above, other expressions use default rules for precision deduction. That is, for the expression `expr(DECIMALV3(a, b))`, the result type is also DECIMALV3(a, b).

#### Adjust the result precision

Different users have different accuracy requirements for DECIMALV3. The above rules are the default behavior of Doris. If users **have different accuracy requirements, they can adjust the accuracy in the following ways**:

* If the expected result precision is greater than the default precision, you can adjust the result precision by adjusting the parameter's precision. For example, if the user expects to calculate `AVG(col)` and get DECIMALV3(x, y) as the result, where the type of `col` is DECIMALV3 (a, b), the expression can be rewritten to `AVG(CAST(col as DECIMALV3 (x, y))`.
* If the expected result precision is less than the default precision, the desired precision can be obtained by approximating the output result. For example, if the user expects to calculate `AVG(col)` and get DECIMALV3(x, y) as the result, where the type of `col` is DECIMALV3(a, b), the expression can be rewritten as `ROUND(AVG(col), y)`.

### Why DECIMALV3 is required

DECIMALV3 in Doris is a real high-precision fixed-point number. Compared with the old version of Decimal, DecimalV3 has the following core advantages:
1. It can represent a wider range. The value ranges of both precision and scale in DECIMALV3 have been significantly expanded.
2. Higher performance. The old version of DECIMAL requires 16 bytes in memory and 12 bytes in storage, while DECIMALV3 has made adaptive adjustments as shown below.
```
+----------------------+------------------------------+
|     precision        | Space occupied (memory/disk) |
+----------------------+------------------------------+
| 0 < precision <= 8   |            4 bytes           |
+----------------------+------------------------------+
| 8 < precision <= 18  |            8 bytes           |
+----------------------+------------------------------+
| 18 < precision <= 38 |           16 bytes           |
+----------------------+------------------------------+
```
3. More complete precision deduction. For different expressions, different precision inference rules are applied to deduce the precision of the results.

### keywords
DECIMALV3
