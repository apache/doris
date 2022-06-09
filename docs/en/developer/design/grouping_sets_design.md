---
{
    "title": "GROUPING SETS DESIGN",
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
# GROUPING SETS DESIGN

## 1. GROUPING SETS Background

The `CUBE`, `ROLLUP`, and `GROUPING` `SETS` extensions to SQL make querying and reporting easier and faster. `CUBE`, `ROLLUP`, and grouping sets produce a single result set that is equivalent to a `UNION` `ALL` of differently grouped rows. `ROLLUP` calculates aggregations such as `SUM`, `COUNT`, `MAX`, `MIN`, and `AVG` at increasing levels of aggregation, from the most detailed up to a grand total. `CUBE` is an extension similar to `ROLLUP`, enabling a single statement to calculate all possible combinations of aggregations. The `CUBE`, `ROLLUP`, and the `GROUPING` `SETS` extension lets you specify just the groupings needed in the `GROUP` `BY` clause. This allows efficient analysis across multiple dimensions without performing a `CUBE` operation. Computing a `CUBE` creates a heavy processing load, so replacing cubes with grouping sets can significantly increase performance.
To enhance performance, `CUBE`, `ROLLUP`, and `GROUPING SETS` can be parallelized: multiple processes can simultaneously execute all of these statements. These capabilities make aggregate calculations more efficient, thereby enhancing database performance, and scalability.

The three `GROUPING` functions help you identify the group each row belongs to and enable sorting subtotal rows and filtering results.

### 1.1 GROUPING SETS Syntax

`GROUPING SETS` syntax lets you define multiple groupings in the same query. `GROUP BY` computes all the groupings specified and combines them with `UNION ALL`. For example, consider the following statement:

```
SELECT k1, k2, SUM( k3 ) FROM t GROUP BY GROUPING SETS ( (k1, k2), (k1), (k2), ( ) );
```


This statement is equivalent to:

```
SELECT k1, k2, SUM( k3 ) FROM t GROUP BY k1, k2
UNION
SELECT k1, null, SUM( k3 ) FROM t GROUP BY k1
UNION
SELECT null, k2, SUM( k3 ) FROM t GROUP BY k2
UNION
SELECT null, null, SUM( k3 ) FROM t
```

This is an example of real query:

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

### 1.2 ROLLUP Syntax

`ROLLUP` enables a `SELECT` statement to calculate multiple levels of subtotals across a specified group of dimensions. It also calculates a grand total. `ROLLUP` is a simple extension to the `GROUP` `BY` clause, so its syntax is extremely easy to use. The `ROLLUP` extension is highly efficient, adding minimal overhead to a query.

`ROLLUP` appears in the `GROUP` `BY` clause in a `SELECT` statement. Its form is:

```
SELECT a, b,c, SUM( d ) FROM tab1 GROUP BY ROLLUP(a,b,c)
```

This statement is equivalent to GROUPING SETS as followed:

```
GROUPING SETS (
(a,b,c),
( a, b ),
( a),
( )
)
```

### 1.3 CUBE Syntax

Like `ROLLUP`   `CUBE` generates all the subtotals that could be calculated for a data cube with the specified dimensions.

```
SELECT a, b,c, SUM( d ) FROM tab1 GROUP BY CUBE(a,b,c)
```

e.g.  CUBE ( a, b, c )  is equivalent to GROUPING SETS as followed:

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

### 1.4 GROUPING and GROUPING_ID Function

Indicates whether a specified column expression in a `GROUP BY` list is aggregated or not. `GROUPING `returns 1 for aggregated or 0 for not aggregated in the result set. `GROUPING` can be used only in the `SELECT` list, `HAVING`, and `ORDER BY` clauses when `GROUP BY` is specified.

`GROUPING_ID` describes which of a list of expressions are grouped in a row produced by a `GROUP BY` query. The `GROUPING_ID` function simply returns the decimal equivalent of the binary value formed as a result of the concatenation of the values returned by the `GROUPING` functions.

Each `GROUPING_ID` argument must be an element of the `GROUP BY` list. `GROUPING_ID ()` returns an **integer** bitmap whose lowest N bits may be lit. A lit **bit** indicates the corresponding argument is not a grouping column for the given output row. The lowest-order **bit** corresponds to argument N, and the N-1th lowest-order **bit** corresponds to argument 1. If the column is a grouping column the bit is 0 else is 1.

For example:

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

grouping sets result:

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
### 1.5 Composition and nesting of GROUPING SETS

First of all, a GROUP BY clause is essentially a special case of GROUPING SETS, for example:

```
   GROUP BY a
is equivalent to:
   GROUP BY GROUPING SETS((a))
also,
   GROUP BY a,b,c
is equivalent to:
   GROUP BY GROUPING SETS((a,b,c))
```

Similarly, CUBE and ROLLUP can be expanded into GROUPING SETS, so the various combinations and nesting of GROUP BY, CUBE, ROLLUP, GROUPING SETS are essentially the combination and nesting of GROUPING SETS.

For GROUPING SETS nesting, it is semantically equivalent to writing the statements inside the nest directly outside. (ref:<https://www.brytlyt.com/documentation/data-manipulation-dml/grouping-sets-rollup-cube/>) mentions: 

```
The CUBE and ROLLUP constructs can be used either directly in the GROUP BY clause, or nested inside a GROUPING SETS clause. If one GROUPING SETS clause is nested inside another, the effect is the same as if all the elements of the inner clause had been written directly in the outer clause.
```

For a combined list of multiple GROUPING SETS, many databases consider it a cross product relationship.

for example:

```
GROUP BY a, CUBE (b, c), GROUPING SETS ((d), (e))

is equivalent to:

GROUP BY GROUPING SETS (
(a, b, c, d), (a, b, c, e),
(a, b, d),    (a, b, e),
(a, c, d),    (a, c, e),
(a, d),       (a, e)
)
```

For the combination and nesting of GROUPING SETS, each database support is not the same. For example snowflake does not support any combination and nesting.
（<https://docs.snowflake.net/manuals/sql-reference/constructs/group-by.html>）

Oracle supports both composition and nesting.
（<https://docs.oracle.com/cd/B19306_01/server.102/b14223/aggreg.htm#i1006842>）

Presto supports composition, but not nesting.
（<https://prestodb.github.io/docs/current/sql/select.html>）

## 2. Object

Support `GROUPING SETS`,  `ROLLUP` and `CUBE ` syntax, implements 1.1, 1.2, 1.3 1.4, 1.5, not support the combination
 and nesting of GROUPING SETS in current version.

### 2.1 GROUPING SETS Syntax

```
SELECT ...
FROM ...
[ ... ]
GROUP BY GROUPING SETS ( groupSet [ , groupSet [ , ... ] ] )
[ ... ]

groupSet ::= { ( expr  [ , expr [ , ... ] ] )}

<expr>
Expression, column name.
```

### 2.2 ROLLUP Syntax

```
SELECT ...
FROM ...
[ ... ]
GROUP BY ROLLUP ( expr  [ , expr [ , ... ] ] )
[ ... ]

<expr>
Expression, column name.
```

### 2.3 CUBE Syntax

```
SELECT ...
FROM ...
[ ... ]
GROUP BY CUBE ( expr  [ , expr [ , ... ] ] )
[ ... ]

<expr>
Expression, column name.
```

## 3. Implementation

### 3.1 Overall Design Approaches

For `GROUPING SET`  is equivalent to the `UNION` of  `GROUP BY` . So we can expand input rows, and run an GROUP BY on these rows.

For example:

```
SELECT a, b FROM src GROUP BY a, b GROUPING SETS ((a, b), (a), (b), ());
```

Data in table src:

```
1, 2
3, 4
```

Base on  GROUPING SETS , we can expend the input to:

```
1, 2       (GROUPING_ID: a, b -> 00 -> 0)
1, null    (GROUPING_ID: a, null -> 01 -> 1)
null, 2    (GROUPING_ID: null, b -> 10 -> 2)
null, null (GROUPING_ID: null, null -> 11 -> 3)

3, 4       (GROUPING_ID: a, b -> 00 -> 0)
3, null    (GROUPING_ID: a, null -> 01 -> 1)
null, 4    (GROUPING_ID: null, b -> 10 -> 2)
null, null (GROUPING_ID: null, null -> 11 -> 3)
```

And then use those row as input, then GROUP BY  a, b, GROUPING_ID

### 3.2 Example

Table t:

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

for the query:

```
SELECT k1, k2, GROUPING_ID(k1,k2), SUM(k3) FROM t GROUP BY GROUPING SETS ((k1, k2), (k1), (k2), ());
```

First, expand the input, every row expand into 4 rows ( the size of GROUPING SETS), and insert GROUPING_ID column

e.g.  a, A, 1 expanded to:

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

Finally, all rows expended as follows (32 rows):

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

now GROUP BY k1, k2, GROUPING_ID(k1,k2):

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

The result is equivalent to the UNION ALL

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

### 3.3 FE 

#### 3.3.1 Tasks

1. Add GroupByClause, replace groupingExprs.
2. Add Grouping Sets, Cube and RollUp syntax.
3. Add GroupByClause in SelectStmt.
4. Add GroupingFunctionCallExpr, implements grouping grouping_id function call
5. Add VirtualSlot, generate the map of virtual slots and real slots
6. add virtual column GROUPING_ID and other virtual columns generated by grouping and grouping_id, insert into groupingExprs,
7. Add a PlanNode, name as RepeatNode. For GroupingSets aggregation insert RepeatNode to the plan.

#### 3.3.2 Tuple

In order to add GROUPING_ID to groupingExprs in GroupByClause, need to create virtual SlotRef, also, need tot create a tuple for this slot, named GROUPING\_\_ID Tuple.

For the plannode RepeatNode, its input are all the tuples of its children and its output tuple are the repeat data and GROUPING_ID.


#### 3.3.3 Expression and Function Substitution

expr -> if(bitand(pos, grouping_id)=0, expr, null) for expr in extension grouping clause
grouping_id() -> grouping_id(grouping_id) for grouping_id function

### 3.4 BE

#### 3.4.1 Tasks

1. Add RepeatNode executor, expend the input data and append GROUPING_ID to every row
2. Implements grouping_id() and grouping() function.
