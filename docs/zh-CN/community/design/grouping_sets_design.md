---
{
    "title": "GROUPING SETS 设计文档",
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

# GROUPING SETS 设计文档

## 1. GROUPING SETS 相关背景知识

### 1.1 GROUPING SETS 子句

GROUP BY GROUPING SETS 是对 GROUP BY 子句的扩展，它能够在一个 GROUP BY 子句中一次实现多个集合的分组。其结果等价于将多个相应 GROUP BY 子句进行 UNION 操作。

特别地，一个空的子集意味着将所有的行聚集到一个分组。
GROUP BY 子句是只含有一个元素的 GROUP BY GROUPING SETS 的特例。

例如，GROUPING SETS 语句：

```
SELECT k1, k2, SUM( k3 ) FROM t GROUP BY GROUPING SETS ( (k1, k2), (k1), (k2), ( ) );
```

其查询结果等价于：

```
SELECT k1, k2, SUM( k3 ) FROM t GROUP BY k1, k2
UNION
SELECT k1, null, SUM( k3 ) FROM t GROUP BY k1
UNION
SELECT null, k2, SUM( k3 ) FROM t GROUP BY k2
UNION
SELECT null, null, SUM( k3 ) FROM t
```

下面是一个实际数据的例子：

```
mysql> SELECT * FROM t;
+------+------+------+
| k1   | k2   | k3   |
+------+------+------+
| a    | A    |    1 |
| a    | A    |    2 |
| a    | B    |    1 |
| a    | B    |    3 |
| b    | A    |    1 |
| b    | A    |    4 |
| b    | B    |    1 |
| b    | B    |    5 |
+------+------+------+
8 rows in set (0.01 sec)

mysql> SELECT k1, k2, SUM(k3) FROM t GROUP BY GROUPING SETS ( (k1, k2), (k2), (k1), ( ) );
+------+------+-----------+
| k1   | k2   | sum(`k3`) |
+------+------+-----------+
| b    | B    |         6 |
| a    | B    |         4 |
| a    | A    |         3 |
| b    | A    |         5 |
| NULL | B    |        10 |
| NULL | A    |         8 |
| a    | NULL |         7 |
| b    | NULL |        11 |
| NULL | NULL |        18 |
+------+------+-----------+
9 rows in set (0.06 sec)
```

### 1.2 ROLLUP 子句

ROLLUP 是对 GROUPING SETS 的扩展。

```
SELECT a, b,c, SUM( d ) FROM tab1 GROUP BY ROLLUP(a,b,c)
```

这个 ROLLUP 等价于下面的 GROUPING SETS：

```
GROUPING SETS (
(a,b,c),
( a, b ),
( a),
( )
)
```

### 1.3 CUBE 子句

CUBE 也是对 GROUPING SETS 的扩展。

```
CUBE ( e1, e2, e3, ... )
```

其含义是 GROUPING SETS 后面列表中的所有子集。

例如，CUBE ( a, b, c ) 等价于下面的 GROUPING SETS：

```
GROUPING SETS (
( a, b, c ),
( a, b ),
( a,    c ),
( a       ),
(    b, c ),
(    b    ),
(       c ),
(         )
)
```

### 1.4 GROUPING 和 GROUPING_ID 函数
当我们没有统计某一列时，它的值显示为 NULL，这也可能是列本身就有 NULL 值，这就需要一种方法区分是没有统计还是值本来就是 NULL。为此引入 GROUPING 和 GROUPING_ID 函数。
GROUPING(column:Column) 函数用于区分分组后的单个列是普通列和聚合列。如果是聚合列，则返回1，反之，则是0. GROUPING() 只能有一个参数列。

GROUPING_ID(column1, column2) 则根据指定的column 顺序，否则根据聚合的时候给的集合的元素顺序，计算出一个列列表的 bitmap 值，一个列如果是聚合列为0，否则为1. GROUPING_ID()函数返回位向量的十进制值。
比如 [0 1 0] ->2 从下列第三个查询可以看到这种对应关系

例如，对于下面的表：

```
mysql> select * from t;
+------+------+------+
| k1   | k2   | k3   |
+------+------+------+
| a    | A    |    1 |
| a    | A    |    2 |
| a    | B    |    1 |
| a    | B    |    3 |
| b    | A    |    1 |
| b    | A    |    4 |
| b    | B    |    1 |
| b    | B    |    5 |
+------+------+------+
```

grouping sets 的结果如下：

```
mysql> SELECT k1, k2, GROUPING(k1), GROUPING(k2), SUM(k3) FROM t GROUP BY GROUPING SETS ( (k1, k2), (k2), (k1), ( ) );
+------+------+----------------+----------------+-----------+
| k1   | k2   | grouping(`k1`) | grouping(`k2`) | sum(`k3`) |
+------+------+----------------+----------------+-----------+
| a    | A    |              0 |              0 |         3 |
| a    | B    |              0 |              0 |         4 |
| a    | NULL |              0 |              1 |         7 |
| b    | A    |              0 |              0 |         5 |
| b    | B    |              0 |              0 |         6 |
| b    | NULL |              0 |              1 |        11 |
| NULL | A    |              1 |              0 |         8 |
| NULL | B    |              1 |              0 |        10 |
| NULL | NULL |              1 |              1 |        18 |
+------+------+----------------+----------------+-----------+
9 rows in set (0.02 sec)

mysql> SELECT k1, k2, GROUPING_ID(k1,k2), SUM(k3) FROM t GROUP BY GROUPING SETS ( (k1, k2), (k2), (k1), ( ) );
+------+------+-------------------------+-----------+
| k1   | k2   | grouping_id(`k1`, `k2`) | sum(`k3`) |
+------+------+-------------------------+-----------+
| a    | A    |                       0 |         3 |
| a    | B    |                       0 |         4 |
| a    | NULL |                       1 |         7 |
| b    | A    |                       0 |         5 |
| b    | B    |                       0 |         6 |
| b    | NULL |                       1 |        11 |
| NULL | A    |                       2 |         8 |
| NULL | B    |                       2 |        10 |
| NULL | NULL |                       3 |        18 |
+------+------+-------------------------+-----------+
9 rows in set (0.02 sec)

mysql> SELECT k1, k2, grouping(k1), grouping(k2), GROUPING_ID(k1,k2), SUM(k4) FROM t GROUP BY GROUPING SETS ( (k1, k2), (k2), (k1), ( ) ) order by k1, k2;
+------+------+----------------+----------------+-------------------------+-----------+
| k1   | k2   | grouping(`k1`) | grouping(`k2`) | grouping_id(`k1`, `k2`) | sum(`k4`) |
+------+------+----------------+----------------+-------------------------+-----------+
| a    | A    |              0 |              0 |                       0 |         3 |
| a    | B    |              0 |              0 |                       0 |         4 |
| a    | NULL |              0 |              1 |                       1 |         7 |
| b    | A    |              0 |              0 |                       0 |         5 |
| b    | B    |              0 |              0 |                       0 |         6 |
| b    | NULL |              0 |              1 |                       1 |        11 |
| NULL | A    |              1 |              0 |                       2 |         8 |
| NULL | B    |              1 |              0 |                       2 |        10 |
| NULL | NULL |              1 |              1 |                       3 |        18 |
+------+------+----------------+----------------+-------------------------+-----------+
9 rows in set (0.02 sec)

```

### 1.5 GROUPING SETS 的组合与嵌套

首先，一个 GROUP BY 子句本质上是一个 GROUPING SETS 的特例, 例如：

```
   GROUP BY a
等同于
   GROUP BY GROUPING SETS((a))
同样地，
   GROUP BY a,b,c
等同于
   GROUP BY GROUPING SETS((a,b,c))
```

同样的，CUBE 和 ROLLUP 也可以展开成 GROUPING SETS，因此 GROUP BY, CUBE, ROLLUP, GROUPING SETS 的各种组合和嵌套本质上就是 GROUPING SETS 的组合与嵌套。

对于 GROUPING SETS 的嵌套，语义上等价于将嵌套内的语句直接写到外面。（参考：<https://www.brytlyt.com/documentation/data-manipulation-dml/grouping-sets-rollup-cube/>），其中写道：

```
The CUBE and ROLLUP constructs can be used either directly in the GROUP BY clause, or nested inside a GROUPING SETS clause. If one GROUPING SETS clause is nested inside another, the effect is the same as if all the elements of the inner clause had been written directly in the outer clause.
```

对于多个 GROUPING SETS 的组合列表，很多数据库认为是叉乘（cross product）的关系。

例如：

```
GROUP BY a, CUBE (b, c), GROUPING SETS ((d), (e))

等同于：

GROUP BY GROUPING SETS (
(a, b, c, d), (a, b, c, e),
(a, b, d),    (a, b, e),
(a, c, d),    (a, c, e),
(a, d),       (a, e)
)
```

对于 GROUPING SETS 的组合与嵌套，各个数据库支持不太一样。例如 snowflake 不支持任何的组合和嵌套。
（<https://docs.snowflake.net/manuals/sql-reference/constructs/group-by.html>）

Oracle 既支持组合，也支持嵌套。
（<https://docs.oracle.com/cd/B19306_01/server.102/b14223/aggreg.htm#i1006842>）

Presto 支持组合，但不支持嵌套。
（<https://prestodb.github.io/docs/current/sql/select.html>）

## 2. 设计目标

从语法上支持 GROUPING SETS， ROLLUP 和 CUBE。实现上述所述的1.1, 1.2, 1.3 1.4.

对于1.6 GROUPING SETS 的组合与嵌套 先不实现。

具体语法列出如下：

### 2.1 GROUPING SETS 语法

```
SELECT ...
FROM ...
[ ... ]
GROUP BY GROUPING SETS ( groupSet [ , groupSet [ , ... ] ] )
[ ... ]

groupSet ::= { ( expr  [ , expr [ , ... ] ] )}

<expr>
各种表达式，包括列名.

```

### 2.2 ROLLUP 语法

```
SELECT ...
FROM ...
[ ... ]
GROUP BY ROLLUP ( expr  [ , expr [ , ... ] ] )
[ ... ]

<expr>
各种表达式，包括列名.

```

### 2.3 CUBE 语法

```
SELECT ...
FROM ...
[ ... ]
GROUP BY CUBE ( expr  [ , expr [ , ... ] ] )
[ ... ]

<expr>
各种表达式，包括列名.

```

## 3. 实现方案

### 3.1 整体思路

既然 GROUPING SET 子句逻辑上等价于多个相应 GROUP BY 子句的 UNION，可以通过扩展输入行(此输入行已经是通过下推条件过滤和投影后的), 在此基础上进行一个单一的 GROUP BY 操作来达到目的。

关键是怎样扩展输入行呢？下面举例说明：

例如，对应下面的语句：

```
SELECT a, b FROM src GROUP BY a, b GROUPING SETS ((a, b), (a), (b), ());

```

假定 src 表的数据如下：

```
1, 2
3, 4

```

根据 GROUPING SETS 子句给出的列表，可以将输入行扩展为下面的 8 行 （GROUPING SETS集合数 * 行数, 同时为每行生成对应的 全列的GROUPING_ID: 和其他grouping 函数的值

```
1, 2       (GROUPING_ID: a, b -> 00->0)
1, null    (GUPING_ID: a, null -> 01 -> 1)
null, 2    (GROUPING_ID: null, b -> 10 -> 2)
null, null (GROUPING_ID: null, null -> 11 -> 3)

3, 4       (GROUPING_ID: a, b -> 00 -> 0)
3, null    (GROUPING_ID: a, null -> 01 -> 1)
null, 4    (GROUPING_ID: null, b -> 10 -> 2)
null, null (GROUPING_ID: null, null -> 11 -> 3)

```

然后，将上面的 8 行数据作为输入，对 a, b, GROUPING_ID 进行 GROUP BY 操作即可。

### 3.2 具体例子验证说明

假设有一个 t 表，包含如下列和数据：

```
mysql> select * from t;
+------+------+------+
| k1   | k2   | k3   |
+------+------+------+
| a    | A    |    1 |
| a    | A    |    2 |
| a    | B    |    1 |
| a    | B    |    3 |
| b    | A    |    1 |
| b    | A    |    4 |
| b    | B    |    1 |
| b    | B    |    5 |
+------+------+------+
8 rows in set (0.01 sec)

```

对于如下的查询：

```
SELECT k1, k2, GROUPING_ID(k1,k2), SUM(k3) FROM t GROUP BY GROUPING SETS ((k1, k2), (k1), (k2), ());

```

首先，对输入行进行扩展，每行数据扩展成 4 行 (GROUPING SETS子句的集合数目)，同时增加 GROUPING_ID() 列 ：

例如 a, A, 1 扩展后变成下面的 4 行：

```
+------+------+------+-------------------------+
| k1   | k2   | k3   | GROUPING_ID(`k1`, `k2`) |
+------+------+------+-------------------------+
| a    | A    |    1 |                       0 |
| a    | NULL |    1 |                       1 |
| NULL | A    |    1 |                       2 |
| NULL | NULL |    1 |                       3 |
+------+------+------+-------------------------+

```

最终， 全部扩展后的输入行如下（总共 32 行）：

```
+------+------+------+-------------------------+
| k1   | k2   | k3   | GROUPING_ID(`k1`, `k2`) |
+------+------+------+-------------------------+
| a    | A    |    1 |                       0 |
| a    | A    |    2 |                       0 |
| a    | B    |    1 |                       0 |
| a    | B    |    3 |                       0 |
| b    | A    |    1 |                       0 |
| b    | A    |    4 |                       0 |
| b    | B    |    1 |                       0 |
| b    | B    |    5 |                       0 |
| a    | NULL |    1 |                       1 |
| a    | NULL |    1 |                       1 |
| a    | NULL |    2 |                       1 |
| a    | NULL |    3 |                       1 |
| b    | NULL |    1 |                       1 |
| b    | NULL |    1 |                       1 |
| b    | NULL |    4 |                       1 |
| b    | NULL |    5 |                       1 |
| NULL | A    |    1 |                       2 |
| NULL | A    |    1 |                       2 |
| NULL | A    |    2 |                       2 |
| NULL | A    |    4 |                       2 |
| NULL | B    |    1 |                       2 |
| NULL | B    |    1 |                       2 |
| NULL | B    |    3 |                       2 |
| NULL | B    |    5 |                       2 |
| NULL | NULL |    1 |                       3 |
| NULL | NULL |    1 |                       3 |
| NULL | NULL |    1 |                       3 |
| NULL | NULL |    1 |                       3 |
| NULL | NULL |    2 |                       3 |
| NULL | NULL |    3 |                       3 |
| NULL | NULL |    4 |                       3 |
| NULL | NULL |    5 |                       3 |
+------+------+------+-------------------------+
32 rows in set.

```

现在对k1, k2, GROUPING_ID(`k1`, `k2`) 进行 GROUP BY：

```
+------+------+-------------------------+-----------+
| k1   | k2   | grouping_id(`k1`, `k2`) | sum(`k3`) |
+------+------+-------------------------+-----------+
| a    | A    |                       0 |         3 |
| a    | B    |                       0 |         4 |
| a    | NULL |                       1 |         7 |
| b    | A    |                       0 |         5 |
| b    | B    |                       0 |         6 |
| b    | NULL |                       1 |        11 |
| NULL | A    |                       2 |         8 |
| NULL | B    |                       2 |        10 |
| NULL | NULL |                       3 |        18 |
+------+------+-------------------------+-----------+
9 rows in set (0.02 sec)

```

可以看到，其结果与对 GROUPING SETS 子句后每个子集进行 GROUP BY 后再进行 UNION 的结果一致。

```
select k1, k2, sum(k3) from t group by k1, k2
UNION ALL
select NULL, k2, sum(k3) from t group by k2
UNION ALL
select k1, NULL, sum(k3) from t group by k1
UNION ALL
select NULL, NULL, sum(k3) from t;

+------+------+-----------+
| k1   | k2   | sum(`k3`) |
+------+------+-----------+
| b    | B    |         6 |
| b    | A    |         5 |
| a    | A    |         3 |
| a    | B    |         4 |
| a    | NULL |         7 |
| b    | NULL |        11 |
| NULL | B    |        10 |
| NULL | A    |         8 |
| NULL | NULL |        18 |
+------+------+-----------+
9 rows in set (0.06 sec)

```

### 3.3 FE 规划阶段

#### 3.3.1 主要任务

1. 引入 GroupByClause 类，封装 Group By 相关信息，替换原有的 groupingExprs.
2. 增加 Grouping Sets, Cube 和 RollUp 的语法支持和语法检查、错误处理和错误信息；
3. 在 SelectStmt 类中增加 GroupByClause 成员；
4. 引入 GroupingFunctionCallExpr 类，封装grouping 和grouping_id 函数调用
5. 引入 VirtualSlot 类，封装grouping，grouping_id  生成的虚拟列和实际列的对应关系
6. 增加虚拟列 GROUPING_ID 和其他grouping，grouping_id 函数对应的虚拟列，并将此列加入到原有的 groupingExprs 表达式列表中；
7. 增加一个 PlanNode，考虑更通用的功能，命名为 RepeatNode。对于 GroupingSets 的聚合，在执行计划中插入 RepeatNode。

#### 3.3.2 Tuple

在 GroupByClause 类中为了将 GROUPING_ID 加到 groupingExprs 表达式列表中，需要创建 virtual SlotRef, 相应的，需要对这个 slot 创建一个 tuple, 叫 GROUPING_ID Tuple。

对于 RepeatNode 这个执行计划，其输入是子节点的所有 tuple， 输出的 tuple 除了 repeat 子节点的数据外，还需要填写 GROUPING_ID 和其他grouping，grouping_id 对应的虚拟列，因此。


### 3.4 BE 查询执行阶段

主要任务：

1. 通过 RepeatNode 的执行类，增加扩展输入行的逻辑，其功能是在聚合之前将原有数据进行 repeat：对每行增加一列 GROUPING_ID， 然后按照 GroupingSets 中的集合数进行 repeat，并对对应列置为 null。根据grouping list设置新增虚拟列的值
2. 实现 grouping_id() 和grouping() 函数。




