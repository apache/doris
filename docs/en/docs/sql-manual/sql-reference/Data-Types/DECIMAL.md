---
{
    "title": "DECIMAL",
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

## DECIMAL

<version since="1.2.1">

DECIMAL

</version>

### Description
DECIMAL (M [,D])

High-precision fixed-point number, M represents the total number of significant digits, and D represents the scale.

The range of M is [1, 38], and the range of D is [0, precision].

The default value is DECIMAL(9, 0).

### Precision Deduction

DECIMAL has a very complex set of type inference rules. For different expressions, different rules will be applied for precision inference.

#### Arithmetic Expressions

* Plus / Minus: DECIMAL(a, b) + DECIMAL(x, y) -> DECIMAL(max(a - b, x - y) + max(b, y) + 1, max(b, y)).
* Multiply: DECIMAL(a, b) + DECIMAL(x, y) -> DECIMAL(a + x, b + y).
* Divide: DECIMAL(p1, s1) + DECIMAL(p2, s2) -> DECIMAL(p1 + s2 + div_precision_increment, s1 + div_precision_increment).div_precision_increment default 4.
It is worth noting that the process of division calculation is as follows:
DECIMAL(p1, s1) / DECIMAL(p2, s2) is first converted to DECIMAL(p1 + s2 + div_precision_increment, s1 + s2) / DECIMAL(p2, s2) and then the calculation is performed. Therefore, it is possible that DECIMAL(p1 + s2 + div_precision_increment, s1 + div_precision_increment) satisfies the range of DECIMAL, 
but due to the conversion to DECIMAL(p1 + s2 + div_precision_increment, s1 + s2), 
it exceeds the range. Currently, Doris handles this by converting it to Double for calculation.
#### Aggregation functions

* SUM / MULTI_DISTINCT_SUM: SUM(DECIMAL(a, b)) -> DECIMAL(38, b).
* AVG: AVG(DECIMAL(a, b)) -> DECIMAL(38, max(b, 4)).

#### Default rules

Except for the expressions mentioned above, other expressions use default rules for precision deduction. That is, for the expression `expr(DECIMAL(a, b))`, the result type is also DECIMAL(a, b).

#### Adjust the result precision

Different users have different accuracy requirements for DECIMAL. The above rules are the default behavior of Doris. If users **have different accuracy requirements, they can adjust the accuracy in the following ways**:

* If the expected result precision is greater than the default precision, you can adjust the result precision by adjusting the parameter's precision. For example, if the user expects to calculate `AVG(col)` and get DECIMAL(x, y) as the result, where the type of `col` is DECIMAL (a, b), the expression can be rewritten to `AVG(CAST(col as DECIMAL (x, y))`.
* If the expected result precision is less than the default precision, the desired precision can be obtained by approximating the output result. For example, if the user expects to calculate `AVG(col)` and get DECIMAL(x, y) as the result, where the type of `col` is DECIMAL(a, b), the expression can be rewritten as `ROUND(AVG(col), y)`.

### Why DECIMAL is required

DECIMAL in Doris is a real high-precision fixed-point number. Decimal has the following core advantages:
1. It can represent a wider range. The value ranges of both precision and scale in DECIMAL have been significantly expanded.
2. Higher performance. The old version of DECIMAL requires 16 bytes in memory and 12 bytes in storage, while DECIMAL has made adaptive adjustments as shown below.
```
+----------------------+------------------------------+
|     precision        | Space occupied (memory/disk) |
+----------------------+------------------------------+
| 0 < precision <= 9   |            4 bytes           |
+----------------------+------------------------------+
| 9 < precision <= 18  |            8 bytes           |
+----------------------+------------------------------+
| 18 < precision <= 38 |           16 bytes           |
+----------------------+------------------------------+
```
3. More complete precision deduction. For different expressions, different precision inference rules are applied to deduce the precision of the results.

### keywords
DECIMAL
