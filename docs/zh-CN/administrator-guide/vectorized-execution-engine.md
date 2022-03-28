---
{
    "title": "[Experimental] 向量化执行引擎",
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

# 向量化执行引擎

向量化执行引擎 是 Doris 当前版本加入的实验性功能。目标是为了替换当前Doris的行式的SQL执行引擎，充分释放现代CPU的计算能力，突破Doris在SQL执行引擎上的性能短板。

它的具体设计、实现和效果可以参阅 [ISSUE 6238](https://github.com/apache/incubator-doris/issues/6238)。


## 原理

当前的Doris的SQL执行引擎是基于行式内存格式，基于传统的火山模型进行设计，在进行SQL算子与函数运算的时候存在大量的非必要的开销：
1. 类型丢失导致的虚函数的调用，函数无法进行内联优化
2. Cache亲和度差，代码和数据Cache的局部性原理无法得到充分利用
3. 无法利用现代CPU的向量化能力将计算SIMD化
4. CPU的分支预测，预取内存不友好 

![image.png](/images/vectorized-execution-engine1.png)

由此带来的一系列开销导致当前Doris执行引擎效率低下，并不适应现代CPU的体系结构。


而如下图所示（引用自[Column-Oriented
Database Systems](https://web.stanford.edu/class/cs346/2015/notes/old/column.pdf)），向量化执行引擎基于现代CPU的特点与火山模型的执行特点，重新设计列式存储系统的SQL执行引擎：

![image.png](/images/vectorized-execution-engine2.png)

1. 重新组织内存的数据结构，用**Column**替换**Tuple**，提高了计算时Cache亲和度，分支预测与预取内存的友好度
2. 分批进行类型判断，在本次批次中都使用类型判断时确定的类型。将每一行类型判断的虚函数开销分摊到批量级别。
3. 通过批级别的类型判断，消除了虚函数的调用，让编译器有函数内联以及SIMD优化的机会

从而大大提高了CPU在SQL执行时的效率，提升了SQL查询的性能。

## 使用方式

### 设置Session变量

#### enable_vectorized_engine
将session变量`enable_vectorized_engine `设置为`true`，则FE在进行查询规划时就会默认将SQL算子与SQL表达式转换为向量化的执行计划。

```
set enable_vectorized_engine = true;
```

#### batch_size
`batch_size`代表了SQL算子每次进行批量计算的行数。Doris默认的配置为`1024`,这个配置的行数会影响向量化执行引擎的性能与CPU缓存预取的行为。这里推荐配置为`4096`。

```
set batch_size = 4096;
```

### NULL值
由于NULL值在向量化执行引擎中会导致性能劣化。所以在建表时，将对应的列设置为NULL通常会影响向量化执行引擎的性能。**这里推荐使用一些特殊的列值表示NULL值，并在建表时设置列为NOT NULL以充分发挥向量化执行引擎的性能。**

### 查看SQL执行的类型

可以通过`explain`命令来查看当前的SQL是否开启了向量化执行引擎：

```
+-----------------------------+
| Explain String              |
+-----------------------------+
| PLAN FRAGMENT 0             |
|  OUTPUT EXPRS:<slot 0> TRUE |
|   PARTITION: UNPARTITIONED  |
|                             |
|   VRESULT SINK              |
|                             |
|   0:VUNION                  |
|      constant exprs:        |
|          TRUE               |
+-----------------------------+
                                       
```
开启了向量化执行引擎之后，在SQL的执行计划之中会在SQL算子前添加一个`V`的标识。

## 与行存执行引擎的部分差异

在绝大多数场景之中，用户只需要默认打开session变量的开关，就可以透明地使用向量化执行引擎，并且使SQL执行的性能得到提升。但是，**目前的向量化执行引擎在下面一些微小的细节上与原先的行存执行引擎存在不同，需要使用者知晓**。这部分区别分为两类

* **a类** ：行存执行引擎需要被废弃和不推荐使用或依赖的功能
* **b类**： 短期没有在向量化执行引擎上得到支持，但后续会得到开发支持的功能


#### a类
1. Float与Double类型计算可能产生精度误差，仅影响小数点后5位之后的数字。**如果对计算精度有特殊要求，请使用Decimal类型**。
2. DateTime类型不支持秒级别以下的计算或format等各种操作，向量化引擎会直接丢弃秒级别以下毫秒的计算结果。同时也不支持`microseconds_add`等，对毫秒计算的函数。
3. 有符合类型进行编码时，`0`与`-0`在SQL执行中被认为是相等的。这可能会影响`distinct`，`group by`等计算的结果。
4. bitmap/hll 类型在向量化执行引擎中：输入均为NULL，则输出的结果为NULL而不是0。

#### b类
1. 不支持`地理位置函数` ，包含了函数中所有以`ST_`开头的函数。具体请参考官方文档SQL函数的部分。
2. 不支持原有行存执行引擎的`UDF`与`UDAF`。
3. `string/text`类型最大长度支持为1MB，而不是默认的2GB。即当开启向量化引擎后，将无法查询或导入大于1MB的字符串。但如果关闭向量化引擎，则依然可以正常查询和导入。
4. 不支持 `select ... into outfile` 的导出方式。 
5. 不支持extrenal broker外表。
