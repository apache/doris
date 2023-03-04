---
{
    "title": "DECIMALV3",
    "language": "zh-CN"
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

### description
    DECIMALV3(M[,D])
    高精度定点数，M 代表一共有多少个有效数字(precision)，D 代表小数位有多少数字(scale)，
    有效数字 M 的范围是 [1, 38]，小数位数字数量 D 的范围是 [0, precision]。

    默认值为 DECIMALV3(9, 0)。

### 精度推演

DECIMALV3有一套很复杂的类型推演规则，针对不同的表达式，会应用不同规则进行精度推断。

#### 四则运算

* 加法 / 减法：DECIMALV3(a, b) + DECIMALV3(x, y) -> DECIMALV3(max(a - b, x - y) + max(b, y), max(b, y))，即整数部分和小数部分都分别使用两个操作数中较大的值。
* 乘法：DECIMALV3(a, b) + DECIMALV3(x, y) -> DECIMALV3(a + x, b + y)。
* 除法：DECIMALV3(a, b) + DECIMALV3(x, y) -> DECIMALV3(a + y, b)。

#### 聚合运算

* SUM / MULTI_DISTINCT_SUM：SUM(DECIMALV3(a, b)) -> DECIMALV3(38, b)。
* AVG：AVG(DECIMALV3(a, b)) -> DECIMALV3(38, max(b, 4))。

#### 默认规则

除上述提到的函数外，其余表达式都使用默认规则进行精度推演。即对于表达式 `expr(DECIMALV3(a, b))`，结果类型同样也是DECIMALV3(a, b)。

#### 调整结果精度

不同用户对DECIMALV3的精度要求各不相同，上述规则为当前Doris的默认行为，如果用户**有不同的精度需求，可以通过以下方式进行精度调整**：
1. 如果期望的结果精度大于默认精度，可以通过调整入参精度来调整结果精度。例如用户期望计算`AVG(col)`得到DECIMALV3(x, y)作为结果，其中`col`的类型为DECIMALV3(a, b)，则可以改写表达式为`AVG(CAST(col as DECIMALV3(x, y)))`。
2. 如果期望的结果精度小于默认精度，可以通过对输出结果求近似得到想要的精度。例如用户期望计算`AVG(col)`得到DECIMALV3(x, y)作为结果，其中`col`的类型为DECIMALV3(a, b)，则可以改写表达式为`ROUND(AVG(col), y)`。

### 为什么需要DECIMALV3

Doris中的DECIMALV3是真正意义上的高精度定点数，相比于老版本的Decimal，DecimalV3有以下核心优势：
1. 可表示范围更大。DECIMALV3中precision和scale的取值范围都进行了明显扩充。
2. 性能更高。老版本的DECIMAL在内存中需要占用16 bytes，在存储中占用12 bytes，而DECIMALV3进行了自适应调整（如下表格）。
```
+----------------------+-------------------+
|     precision        | 占用空间（内存/磁盘）|
+----------------------+-------------------+
| 0 < precision <= 8   |      4 bytes      |
+----------------------+-------------------+
| 8 < precision <= 18  |      8 bytes      |
+----------------------+-------------------+
| 18 < precision <= 38 |     16 bytes      |
+----------------------+-------------------+
```
3. 更完备的精度推演。对于不同的表达式，应用不同的精度推演规则对结果的精度进行推演。

### keywords
    DECIMALV3
