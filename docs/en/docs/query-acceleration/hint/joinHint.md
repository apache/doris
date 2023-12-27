# join hint using document

In the database, "Hint" is an instruction that instructs the query optimizer to execute a plan. By embedding hints in SQL statements, you can influence the optimizer's decision to select the desired execution path. Here is a background example using Hint:

Suppose you have a table that contains a large amount of data, and you know that in some specific circumstances, the join order of the tables in a query may affect query performance. Leading Hint allows you to specify the order of table joins that you want the optimizer to follow.

For example, consider the following SQL query:
```sql
mysql> explain shape plan select * from t1 join t2 on t1.c1 = c2;
+-------------------------------------------+
| Explain String                            |
+-------------------------------------------+
| PhysicalResultSink                        |
| --PhysicalDistribute                      |
| ----PhysicalProject                       |
| ------hashJoin[INNER_JOIN](t1.c1 = t2.c2) |
| --------PhysicalOlapScan[t2]              |
| --------PhysicalDistribute                |
| ----------PhysicalOlapScan[t1]            |
+-------------------------------------------+
7 rows in set (0.06 sec)
```

In the above example, when the execution efficiency is not ideal, we want to adjust the join order without changing the original sql so as to avoid affecting the user's original scene and achieve the purpose of tuning. We can use leading to arbitrarily change the join order of tableA and tableB. For example, it could be written like:
```sql
mysql> explain shape plan select /*+ leading(t2 t1) */ * from t1 join t2 on c1 = c2;
+-----------------------------------------------------------------------------------------------------+
| Explain String(Nereids Planner)                                                                     |
+-----------------------------------------------------------------------------------------------------+
| PhysicalResultSink                                                                                  |
| --PhysicalDistribute                                                                                |
| ----PhysicalProject                                                                                 |
| ------hashJoin[INNER_JOIN] hashCondition=((t1.c1 = t2.c2)) otherCondition=() build RFs:RF0 c1->[c2] |
| --------PhysicalOlapScan[t2] apply RFs: RF0                                                         |
| --------PhysicalDistribute                                                                          |
| ----------PhysicalOlapScan[t1]                                                                      |
|                                                                                                     |
| Hint log:                                                                                           |
| Used: leading(t2 t1)                                                                                |
| UnUsed:                                                                                             |
| SyntaxError:                                                                                        |
+-----------------------------------------------------------------------------------------------------+
12 rows in set (0.06 sec)
```
In this example, the Hint /*+ leading(t2 t1) */ is used. This Hint tells the optimizer to use the specified table (t2) as the driver table in the execution plan, before (t1).

This document mainly describes how to use join related hints in Doris: leading hint, ordered hint and distribute hint
# Leading hint
Leading Hint is used to guide the optimizer in determining the join order of the query plan. In a query, the join order of the tables can affect query performance. Leading Hint allows you to specify the order of table joins that you want the optimizer to follow.

In doris, the syntax is /*+LEADING(tablespec [tablespec]... ) */,leading is surrounded by "/*+" and "*/" and placed directly behind the select in the select statement. Note that the '/' after leading and the selectlist need to be separated by at least one separator such as a space. At least two more tables need to be written before the leadinghint is justified. And any join can be bracketed to explicitly specify the shape of the joinTree. Example:
```sql
mysql> explain shape plan select /*+ leading(t2 t1) */ * from t1 join t2 on c1 = c2;
+------------------------------------------------------------------------------+
| Explain String(Nereids Planner)                                              |
+------------------------------------------------------------------------------+
| PhysicalResultSink                                                           |
| --PhysicalDistribute[DistributionSpecGather]                                 |
| ----PhysicalProject                                                          |
| ------hashJoin[INNER_JOIN] hashCondition=((t1.c1 = t2.c2)) otherCondition=() |
| --------PhysicalOlapScan[t2]                                                 |
| --------PhysicalDistribute[DistributionSpecHash]                             |
| ----------PhysicalOlapScan[t1]                                               |
|                                                                              |
| Hint log:                                                                    |
| Used: leading(t2 t1)                                                         |
| UnUsed:                                                                      |
| SyntaxError:                                                                 |
+------------------------------------------------------------------------------+
12 rows in set (0.01 sec)
```
- If the leadinghint fails to take effect, the explain process will be used to generate the leadinghint. The EXPLAIN process will display whether the Leadinghint takes effect. There are three types of hints:

  - Used: leading hint takes effect normally

  - Unused: Unsupported cases include the feature that the leading specified join order is not equivalent to the original sql or is not supported in this version (see restrictions).

  - SyntaxError: indicates leading hint syntax errors, such as failure to find the corresponding table

- The leading hint syntax creates a left deep tree by default. For example, select /*+ leading(t1 t2 t3) */ * from t1 join t2 on... Specify by default
```sql
      join
     /    \
   join    t3
  /    \
 t1    t2

mysql> explain shape plan select /*+ leading(t1 t2 t3) */ * from t1 join t2 on c1 = c2 join t3 on c2=c3;
+--------------------------------------------------------------------------------+
| Explain String(Nereids Planner)                                                |
+--------------------------------------------------------------------------------+
| PhysicalResultSink                                                             |
| --PhysicalDistribute[DistributionSpecGather]                                   |
| ----PhysicalProject                                                            |
| ------hashJoin[INNER_JOIN] hashCondition=((t2.c2 = t3.c3)) otherCondition=()   |
| --------hashJoin[INNER_JOIN] hashCondition=((t1.c1 = t2.c2)) otherCondition=() |
| ----------PhysicalOlapScan[t1]                                                 |
| ----------PhysicalDistribute[DistributionSpecHash]                             |
| ------------PhysicalOlapScan[t2]                                               |
| --------PhysicalDistribute[DistributionSpecHash]                               |
| ----------PhysicalOlapScan[t3]                                                 |
|                                                                                |
| Hint log:                                                                      |
| Used: leading(t1 t2 t3)                                                        |
| UnUsed:                                                                        |
| SyntaxError:                                                                   |
+--------------------------------------------------------------------------------+
15 rows in set (0.00 sec)
```
The join tree shape is also allowed to be specified using braces. For example: /*+ leading(t1 {t2 t3}) */  
```sql
      join
     /    \
    t1    join
  /    \
  t2    t3

mysql> explain shape plan select /*+ leading(t1 {t2 t3}) */ * from t1 join t2 on c1 = c2 join t3 on c2=c3;
+----------------------------------------------------------------------------------+
| Explain String(Nereids Planner)                                                  |
+----------------------------------------------------------------------------------+
| PhysicalResultSink                                                               |
| --PhysicalDistribute[DistributionSpecGather]                                     |
| ----PhysicalProject                                                              |
| ------hashJoin[INNER_JOIN] hashCondition=((t1.c1 = t2.c2)) otherCondition=()     |
| --------PhysicalOlapScan[t1]                                                     |
| --------PhysicalDistribute[DistributionSpecHash]                                 |
| ----------hashJoin[INNER_JOIN] hashCondition=((t2.c2 = t3.c3)) otherCondition=() |
| ------------PhysicalOlapScan[t2]                                                 |
| ------------PhysicalDistribute[DistributionSpecHash]                             |
| --------------PhysicalOlapScan[t3]                                               |
|                                                                                  |
| Hint log:                                                                        |
| Used: leading(t1 { t2 t3 })                                                      |
| UnUsed:                                                                          |
| SyntaxError:                                                                     |
+----------------------------------------------------------------------------------+
15 rows in set (0.02 sec)
```

- When a view is used as an alias in joinReorder, the corresponding view can be specified as the leading parameter. Example:
```sql
mysql> explain shape plan select /*+ leading(alias t1) */ count(*) from t1 join (select c2 from t2 join t3 on t2.c2 = t3.c3) as alias on t1.c1 = alias.c2;
  +--------------------------------------------------------------------------------------+
  | Explain String(Nereids Planner)                                                      |
  +--------------------------------------------------------------------------------------+
  | PhysicalResultSink                                                                   |
  | --hashAgg[GLOBAL]                                                                    |
  | ----PhysicalDistribute[DistributionSpecGather]                                       |
  | ------hashAgg[LOCAL]                                                                 |
  | --------PhysicalProject                                                              |
  | ----------hashJoin[INNER_JOIN] hashCondition=((t1.c1 = alias.c2)) otherCondition=()  |
  | ------------PhysicalProject                                                          |
  | --------------hashJoin[INNER_JOIN] hashCondition=((t2.c2 = t3.c3)) otherCondition=() |
  | ----------------PhysicalProject                                                      |
  | ------------------PhysicalOlapScan[t2]                                               |
  | ----------------PhysicalDistribute[DistributionSpecHash]                             |
  | ------------------PhysicalProject                                                    |
  | --------------------PhysicalOlapScan[t3]                                             |
  | ------------PhysicalDistribute[DistributionSpecHash]                                 |
  | --------------PhysicalProject                                                        |
  | ----------------PhysicalOlapScan[t1]                                                 |
  |                                                                                      |
  | Hint log:                                                                            |
  | Used: leading(alias t1)                                                              |
  | UnUsed:                                                                              |
  | SyntaxError:                                                                         |
  +--------------------------------------------------------------------------------------+
  21 rows in set (0.06 sec)
```
## Basic cases
  （Note that the column name is related to the table name, for example: only t1 has c1 field, the following example will write t1.c1 directly to c1 for simplicity）
```sql
CREATE DATABASE testleading;
USE testleading;

create table t1 (c1 int, c11 int) distributed by hash(c1) buckets 3 properties('replication_num' = '1');
create table t2 (c2 int, c22 int) distributed by hash(c2) buckets 3 properties('replication_num' = '1');
create table t3 (c3 int, c33 int) distributed by hash(c3) buckets 3 properties('replication_num' = '1');
create table t4 (c4 int, c44 int) distributed by hash(c4) buckets 3 properties('replication_num' = '1');

```  
For a simple example, when we need to exchange the join order of t1 and t2, we only need to add leading(t2 t1) before it, which will be used in the explain
Shows whether the hint is used.
original plan
```sql
mysql> explain shape plan select * from t1 join t2 on t1.c1 = c2;
+-------------------------------------------+
| Explain String                            |
+-------------------------------------------+
| PhysicalResultSink                        |
| --PhysicalDistribute                      |
| ----PhysicalProject                       |
| ------hashJoin[INNER_JOIN](t1.c1 = t2.c2) |
| --------PhysicalOlapScan[t2]              |
| --------PhysicalDistribute                |
| ----------PhysicalOlapScan[t1]            |
+-------------------------------------------+
7 rows in set (0.06 sec)
  ```
Leading plan
```sql
mysql> explain shape plan select /*+ leading(t2 t1) */ * from t1 join t2 on c1 = c2;
+------------------------------------------------------------------------------+
| Explain String(Nereids Planner)                                              |
+------------------------------------------------------------------------------+
| PhysicalResultSink                                                           |
| --PhysicalDistribute[DistributionSpecGather]                                 |
| ----PhysicalProject                                                          |
| ------hashJoin[INNER_JOIN] hashCondition=((t1.c1 = t2.c2)) otherCondition=() |
| --------PhysicalOlapScan[t2]                                                 |
| --------PhysicalDistribute[DistributionSpecHash]                             |
| ----------PhysicalOlapScan[t1]                                               |
|                                                                              |
| Hint log:                                                                    |
| Used: leading(t2 t1)                                                         |
| UnUsed:                                                                      |
| SyntaxError:                                                                 |
+------------------------------------------------------------------------------+
12 rows in set (0.00 sec)
  ```
hint effect display
（Used unused）
If the leading hint has a syntax error, the corresponding information is displayed in the syntax Error when explaining, but the plan is generated as usual, just without leading
```sql
mysql> explain shape plan select /*+ leading(t2 t3) */ * from t1 join t2 on t1.c1 = c2;
+--------------------------------------------------------+
| Explain String                                         |
+--------------------------------------------------------+
| PhysicalResultSink                                     |
| --PhysicalDistribute                                   |
| ----PhysicalProject                                    |
| ------hashJoin[INNER_JOIN](t1.c1 = t2.c2)              |
| --------PhysicalOlapScan[t1]                           |
| --------PhysicalDistribute                             |
| ----------PhysicalOlapScan[t2]                         |
|                                                        |
| Used:                                                  |
| UnUsed:                                                |
| SyntaxError: leading(t2 t3) Msg:can not find table: t3 |
+--------------------------------------------------------+
11 rows in set (0.01 sec)
  ```
## more cases
### Left deep tree
leading generates a left deep tree by default when we don't use any parentheses
```sql
mysql> explain shape plan select /*+ leading(t1 t2 t3) */ * from t1 join t2 on t1.c1 = c2 join t3 on c2 = c3;
+--------------------------------------------------------------------------------+
| Explain String(Nereids Planner)                                                |
+--------------------------------------------------------------------------------+
| PhysicalResultSink                                                             |
| --PhysicalDistribute[DistributionSpecGather]                                   |
| ----PhysicalProject                                                            |
| ------hashJoin[INNER_JOIN] hashCondition=((t2.c2 = t3.c3)) otherCondition=()   |
| --------hashJoin[INNER_JOIN] hashCondition=((t1.c1 = t2.c2)) otherCondition=() |
| ----------PhysicalOlapScan[t1]                                                 |
| ----------PhysicalDistribute[DistributionSpecHash]                             |
| ------------PhysicalOlapScan[t2]                                               |
| --------PhysicalDistribute[DistributionSpecHash]                               |
| ----------PhysicalOlapScan[t3]                                                 |
|                                                                                |
| Hint log:                                                                      |
| Used: leading(t1 t2 t3)                                                        |
| UnUsed:                                                                        |
| SyntaxError:                                                                   |
+--------------------------------------------------------------------------------+
15 rows in set (0.10 sec)
  ```
### Right deep tree
When we want to make the shape of the plan into a right deep tree, bushy tree or zigzag tree, we only need to add curly brackets to limit the shape of the plan, instead of using swap to adjust from the left deep tree step by step like oracle.
```sql
mysql> explain shape plan select /*+ leading(t1 {t2 t3}) */ * from t1 join t2 on t1.c1 = c2 join t3 on c2 = c3;
+-----------------------------------------------+
| Explain String                                |
+-----------------------------------------------+
| PhysicalResultSink                            |
| --PhysicalDistribute                          |
| ----PhysicalProject                           |
| ------hashJoin[INNER_JOIN](t1.c1 = t2.c2)     |
| --------PhysicalOlapScan[t1]                  |
| --------PhysicalDistribute                    |
| ----------hashJoin[INNER_JOIN](t2.c2 = t3.c3) |
| ------------PhysicalOlapScan[t2]              |
| ------------PhysicalDistribute                |
| --------------PhysicalOlapScan[t3]            |
|                                               |
| Used: leading(t1 { t2 t3 })                   |
| UnUsed:                                       |
| SyntaxError:                                  |
+-----------------------------------------------+
14 rows in set (0.02 sec)
  ```
### Bushy tree
```sql
mysql> explain shape plan select /*+ leading({t1 t2} {t3 t4}) */ * from t1 join t2 on t1.c1 = c2 join t3 on c2 = c3 join t4 on c3 = c4;
+-----------------------------------------------+
| Explain String                                |
+-----------------------------------------------+
| PhysicalResultSink                            |
| --PhysicalDistribute                          |
| ----PhysicalProject                           |
| ------hashJoin[INNER_JOIN](t2.c2 = t3.c3)     |
| --------hashJoin[INNER_JOIN](t1.c1 = t2.c2)   |
| ----------PhysicalOlapScan[t1]                |
| ----------PhysicalDistribute                  |
| ------------PhysicalOlapScan[t2]              |
| --------PhysicalDistribute                    |
| ----------hashJoin[INNER_JOIN](t3.c3 = t4.c4) |
| ------------PhysicalOlapScan[t3]              |
| ------------PhysicalDistribute                |
| --------------PhysicalOlapScan[t4]            |
|                                               |
| Used: leading({ t1 t2 } { t3 t4 })            |
| UnUsed:                                       |
| SyntaxError:                                  |
+-----------------------------------------------+
17 rows in set (0.02 sec)
  ```
### zig-zag tree
```sql
mysql> explain shape plan select /*+ leading(t1 {t2 t3} t4) */ * from t1 join t2 on t1.c1 = c2 join t3 on c2 = c3 join t4 on c3 = c4;
+--------------------------------------------------------------------------------------+
| Explain String(Nereids Planner)                                                      |
+--------------------------------------------------------------------------------------+
| PhysicalResultSink                                                                   |
| --PhysicalDistribute[DistributionSpecGather]                                         |
| ----PhysicalProject                                                                  |
| ------hashJoin[INNER_JOIN] hashCondition=((t3.c3 = t4.c4)) otherCondition=()         |
| --------PhysicalDistribute[DistributionSpecHash]                                     |
| ----------hashJoin[INNER_JOIN] hashCondition=((t1.c1 = t2.c2)) otherCondition=()     |
| ------------PhysicalOlapScan[t1]                                                     |
| ------------PhysicalDistribute[DistributionSpecHash]                                 |
| --------------hashJoin[INNER_JOIN] hashCondition=((t2.c2 = t3.c3)) otherCondition=() |
| ----------------PhysicalOlapScan[t2]                                                 |
| ----------------PhysicalDistribute[DistributionSpecHash]                             |
| ------------------PhysicalOlapScan[t3]                                               |
| --------PhysicalDistribute[DistributionSpecHash]                                     |
| ----------PhysicalOlapScan[t4]                                                       |
|                                                                                      |
| Hint log:                                                                            |
| Used: leading(t1 { t2 t3 } t4)                                                       |
| UnUsed:                                                                              |
| SyntaxError:                                                                         |
+--------------------------------------------------------------------------------------+
19 rows in set (0.02 sec)
  ```
## Non-inner join：
When non-inner-joins are encountered, such as Outer join or semi/anti join, leading hint automatically deduces the join mode of each join according to the original sql semantics. If leading hints that differ from the original sql semantics or cannot be generated, they are placed in unused, but do not affect the generation of the normal flow of the plan.

Here are examples of things that can't be exchanged:   
-------- test outer join which can not swap  
--  t1 leftjoin (t2 join t3 on (P23)) on (P12) != (t1 leftjoin t2 on (P12)) join t3 on (P23)
```sql
mysql> explain shape plan select /*+ leading(t1 {t2 t3}) */ * from t1 left join t2 on c1 = c2 join t3 on c2 = c3;
+--------------------------------------------------------------------------------+
| Explain String(Nereids Planner)                                                |
+--------------------------------------------------------------------------------+
| PhysicalResultSink                                                             |
| --PhysicalDistribute[DistributionSpecGather]                                   |
| ----PhysicalProject                                                            |
| ------hashJoin[INNER_JOIN] hashCondition=((t2.c2 = t3.c3)) otherCondition=()   |
| --------hashJoin[INNER_JOIN] hashCondition=((t1.c1 = t2.c2)) otherCondition=() |
| ----------PhysicalOlapScan[t1]                                                 |
| ----------PhysicalDistribute[DistributionSpecHash]                             |
| ------------PhysicalOlapScan[t2]                                               |
| --------PhysicalDistribute[DistributionSpecHash]                               |
| ----------PhysicalOlapScan[t3]                                                 |
|                                                                                |
| Hint log:                                                                      |
| Used:                                                                          |
| UnUsed: leading(t1 { t2 t3 })                                                  |
| SyntaxError:                                                                   |
+--------------------------------------------------------------------------------+
15 rows in set (0.01 sec)
  ```
Below are some examples that can be exchanged and examples that cannot be exchanged, which readers can verify for themselves
```sql
-------- test outer join which can swap
-- (t1 leftjoin t2  on (P12)) innerjoin t3 on (P13) = (t1 innerjoin t3 on (P13)) leftjoin t2  on (P12)
explain shape plan select * from t1 left join t2 on c1 = c2 join t3 on c1 = c3;
explain shape plan select /*+ leading(t1 t3 t2) */ * from t1 left join t2 on c1 = c2 join t3 on c1 = c3;

-- (t1 leftjoin t2  on (P12)) leftjoin t3 on (P13) = (t1 leftjoin t3 on (P13)) leftjoin t2  on (P12)
explain shape plan select * from t1 left join t2 on c1 = c2 left join t3 on c1 = c3;
explain shape plan select /*+ leading(t1 t3 t2) */ * from t1 left join t2 on c1 = c2 left join t3 on c1 = c3;

-- (t1 leftjoin t2  on (P12)) leftjoin t3 on (P23) = t1 leftjoin (t2  leftjoin t3 on (P23)) on (P12)
select /*+ leading(t2 t3 t1) SWAP_INPUT(t1) */ * from t1 left join t2 on c1 = c2 left join t3 on c2 = c3;
explain shape plan select /*+ leading(t1 {t2 t3}) */ * from t1 left join t2 on c1 = c2 left join t3 on c2 = c3;
explain shape plan select /*+ leading(t1 {t2 t3}) */ * from t1 left join t2 on c1 = c2 left join t3 on c2 = c3;

-------- test outer join which can not swap
--  t1 leftjoin (t2  join t3 on (P23)) on (P12) != (t1 leftjoin t2  on (P12)) join t3 on (P23)
-- eliminated to inner join
explain shape plan select /*+ leading(t1 {t2 t3}) */ * from t1 left join t2 on c1 = c2 join t3 on c2 = c3;
explain graph select /*+ leading(t1 t2 t3) */ * from t1 left join (select * from t2 join t3 on c2 = c3) on c1 = c2;

-- test semi join
explain shape plan select * from t1 where c1 in (select c2 from t2);
explain shape plan select /*+ leading(t2 t1) */ * from t1 where c1 in (select c2 from t2);

-- test anti join
explain shape plan select * from t1 where exists (select c2 from t2);
```
## View
In the case of aliases, you can specify the alias as a complete subtree with joinOrder generated from text order.
```sql
mysql>  explain shape plan select /*+ leading(alias t1) */ count(*) from t1 join (select c2 from t2 join t3 on t2.c2 = t3.c3) as alias on t1.c1 = alias.c2;
+--------------------------------------------------------------------------------------+
| Explain String(Nereids Planner)                                                      |
+--------------------------------------------------------------------------------------+
| PhysicalResultSink                                                                   |
| --hashAgg[GLOBAL]                                                                    |
| ----PhysicalDistribute[DistributionSpecGather]                                       |
| ------hashAgg[LOCAL]                                                                 |
| --------PhysicalProject                                                              |
| ----------hashJoin[INNER_JOIN] hashCondition=((t1.c1 = alias.c2)) otherCondition=()  |
| ------------PhysicalProject                                                          |
| --------------hashJoin[INNER_JOIN] hashCondition=((t2.c2 = t3.c3)) otherCondition=() |
| ----------------PhysicalProject                                                      |
| ------------------PhysicalOlapScan[t2]                                               |
| ----------------PhysicalDistribute[DistributionSpecHash]                             |
| ------------------PhysicalProject                                                    |
| --------------------PhysicalOlapScan[t3]                                             |
| ------------PhysicalDistribute[DistributionSpecHash]                                 |
| --------------PhysicalProject                                                        |
| ----------------PhysicalOlapScan[t1]                                                 |
|                                                                                      |
| Hint log:                                                                            |
| Used: leading(alias t1)                                                              |
| UnUsed:                                                                              |
| SyntaxError:                                                                         |
+--------------------------------------------------------------------------------------+
21 rows in set (0.02 sec)
  ```
## mixed with ordered hint
When ordered hint is used together with ordered hint, ordered hint takes effect with a higher priority than leading hint. Example:
```sql
mysql>  explain shape plan select /*+ ORDERED LEADING(t1 t2 t3) */ t1.c1 from t2 join t1 on t1.c1 = t2.c2 join t3 on c2 = c3;
+--------------------------------------------------------------------------------+
| Explain String(Nereids Planner)                                                |
+--------------------------------------------------------------------------------+
| PhysicalResultSink                                                             |
| --PhysicalDistribute[DistributionSpecGather]                                   |
| ----PhysicalProject                                                            |
| ------hashJoin[INNER_JOIN] hashCondition=((t2.c2 = t3.c3)) otherCondition=()   |
| --------hashJoin[INNER_JOIN] hashCondition=((t1.c1 = t2.c2)) otherCondition=() |
| ----------PhysicalProject                                                      |
| ------------PhysicalOlapScan[t2]                                               |
| ----------PhysicalDistribute[DistributionSpecHash]                             |
| ------------PhysicalProject                                                    |
| --------------PhysicalOlapScan[t1]                                             |
| --------PhysicalDistribute[DistributionSpecHash]                               |
| ----------PhysicalProject                                                      |
| ------------PhysicalOlapScan[t3]                                               |
|                                                                                |
| Hint log:                                                                      |
| Used: ORDERED                                                                  |
| UnUsed: leading(t1 t2 t3)                                                      |
| SyntaxError:                                                                   |
+--------------------------------------------------------------------------------+
18 rows in set (0.02 sec)
  ```
## Limitation
- The current version supports only one leadingHint. If leadinghint is used with a subquery, the query will report an error. Example (This example explain will report an error, but will follow the normal path generation plan) :
```sql
mysql>  explain shape plan select /*+ leading(alias t1) */ count(*) from t1 join (select /*+ leading(t3 t2) */ c2 from t2 join t3 on t2.c2 = t3.c3) as alias on t1.c1 = alias.c2;
  +----------------------------------------------------------------------------------------+
  | Explain String(Nereids Planner)                                                        |
  +----------------------------------------------------------------------------------------+
  | PhysicalResultSink                                                                     |
  | --hashAgg[GLOBAL]                                                                      |
  | ----PhysicalDistribute[DistributionSpecGather]                                         |
  | ------hashAgg[LOCAL]                                                                   |
  | --------PhysicalProject                                                                |
  | ----------hashJoin[INNER_JOIN] hashCondition=((t1.c1 = alias.c2)) otherCondition=()    |
  | ------------PhysicalProject                                                            |
  | --------------PhysicalOlapScan[t1]                                                     |
  | ------------PhysicalDistribute[DistributionSpecHash]                                   |
  | --------------PhysicalProject                                                          |
  | ----------------hashJoin[INNER_JOIN] hashCondition=((t2.c2 = t3.c3)) otherCondition=() |
  | ------------------PhysicalProject                                                      |
  | --------------------PhysicalOlapScan[t2]                                               |
  | ------------------PhysicalDistribute[DistributionSpecHash]                             |
  | --------------------PhysicalProject                                                    |
  | ----------------------PhysicalOlapScan[t3]                                             |
  |                                                                                        |
  | Hint log:                                                                              |
  | Used:                                                                                  |
  | UnUsed: leading(alias t1)                                                              |
  | SyntaxError: leading(t3 t2) Msg:one query block can only have one leading clause       |
  +----------------------------------------------------------------------------------------+
  21 rows in set (0.01 sec)
```
 # OrderedHint
- Using ordered hint causes the join tree to be fixed in shape and displayed in text order
- The syntax is /*+ ORDERED */,leading is surrounded by "/*+" and "*/" and placed directly behind the select in the select statement, for example：
```sql
  explain shape plan select /*+ ORDERED */ t1.c1 from t2 join t1 on t1.c1 = t2.c2 join t3 on c2 = c3;
     join
    /    \
   join    t3
  /    \
  t2    t1

mysql> explain shape plan select /*+ ORDERED */ t1.c1 from t2 join t1 on t1.c1 = t2.c2 join t3 on c2 = c3;
+--------------------------------------------------------------------------------+
| Explain String(Nereids Planner)                                                |
+--------------------------------------------------------------------------------+
| PhysicalResultSink                                                             |
| --PhysicalDistribute[DistributionSpecGather]                                   |
| ----PhysicalProject                                                            |
| ------hashJoin[INNER_JOIN] hashCondition=((t2.c2 = t3.c3)) otherCondition=()   |
| --------hashJoin[INNER_JOIN] hashCondition=((t1.c1 = t2.c2)) otherCondition=() |
| ----------PhysicalProject                                                      |
| ------------PhysicalOlapScan[t2]                                               |
| ----------PhysicalDistribute[DistributionSpecHash]                             |
| ------------PhysicalProject                                                    |
| --------------PhysicalOlapScan[t1]                                             |
| --------PhysicalDistribute[DistributionSpecHash]                               |
| ----------PhysicalProject                                                      |
| ------------PhysicalOlapScan[t3]                                               |
|                                                                                |
| Hint log:                                                                      |
| Used: ORDERED                                                                  |
| UnUsed:                                                                        |
| SyntaxError:                                                                   |
+--------------------------------------------------------------------------------+
18 rows in set (0.02 sec)
  ```
- When ordered hint and leading hint are used at the same time, the ordered hint prevails and the leading hint becomes invalid
```sql
mysql> explain shape plan select /*+ ORDERED LEADING(t1 t2 t3) */ t1.c1 from t2 join t1 on t1.c1 = t2.c2 join t3 on c2 = c3;
  +--------------------------------------------------------------------------------+
  | Explain String(Nereids Planner)                                                |
  +--------------------------------------------------------------------------------+
  | PhysicalResultSink                                                             |
  | --PhysicalDistribute[DistributionSpecGather]                                   |
  | ----PhysicalProject                                                            |
  | ------hashJoin[INNER_JOIN] hashCondition=((t2.c2 = t3.c3)) otherCondition=()   |
  | --------hashJoin[INNER_JOIN] hashCondition=((t1.c1 = t2.c2)) otherCondition=() |
  | ----------PhysicalProject                                                      |
  | ------------PhysicalOlapScan[t2]                                               |
  | ----------PhysicalDistribute[DistributionSpecHash]                             |
  | ------------PhysicalProject                                                    |
  | --------------PhysicalOlapScan[t1]                                             |
  | --------PhysicalDistribute[DistributionSpecHash]                               |
  | ----------PhysicalProject                                                      |
  | ------------PhysicalOlapScan[t3]                                               |
  |                                                                                |
  | Hint log:                                                                      |
  | Used: ORDERED                                                                  |
  | UnUsed: leading(t1 t2 t3)                                                      |
  | SyntaxError:                                                                   |
  +--------------------------------------------------------------------------------+
  18 rows in set (0.02 sec)
  ```
# DistributeHint
- Currently, only the distribute Type of the right table can be specified, and only two types are available: [shuffle] and [broadcast]. They are written before the right join table. Brackets and /*+ */ are allowed

- Currently you can use any DistributeHint

- When a DistributeHint whose plan cannot be correctly generated is not displayed, it takes effect based on maximum effort. Finally, the distribute mode that is displayed is mainly explained

- The current distribute version is not combined with leading. It takes effect only when the table specified in DISTRIBUTE is on the right of join.

- Mixed with ordered, the text order is used to set the join order and then specify the expected distribute mode in the corresponding join. Example:

Before use distribute hint:
```sql
mysql> explain shape plan select count(*) from t1 join t2 on t1.c1 = t2.c2;
  +----------------------------------------------------------------------------------+
  | Explain String(Nereids Planner)                                                  |
  +----------------------------------------------------------------------------------+
  | PhysicalResultSink                                                               |
  | --hashAgg[GLOBAL]                                                                |
  | ----PhysicalDistribute[DistributionSpecGather]                                   |
  | ------hashAgg[LOCAL]                                                             |
  | --------PhysicalProject                                                          |
  | ----------hashJoin[INNER_JOIN] hashCondition=((t1.c1 = t2.c2)) otherCondition=() |
  | ------------PhysicalProject                                                      |
  | --------------PhysicalOlapScan[t1]                                               |
  | ------------PhysicalDistribute[DistributionSpecHash]                             |
  | --------------PhysicalProject                                                    |
  | ----------------PhysicalOlapScan[t2]                                             |
  +----------------------------------------------------------------------------------+
  11 rows in set (0.01 sec)
  ```

  after use distribute hint：
  ```sql
mysql> explain shape plan select /*+ ordered */ count(*) from t2 join[broadcast] t1 on t1.c1 = t2.c2;
  +----------------------------------------------------------------------------------+
  | Explain String(Nereids Planner)                                                  |
  +----------------------------------------------------------------------------------+
  | PhysicalResultSink                                                               |
  | --hashAgg[GLOBAL]                                                                |
  | ----PhysicalDistribute[DistributionSpecGather]                                   |
  | ------hashAgg[LOCAL]                                                             |
  | --------PhysicalProject                                                          |
  | ----------hashJoin[INNER_JOIN] hashCondition=((t1.c1 = t2.c2)) otherCondition=() |
  | ------------PhysicalProject                                                      |
  | --------------PhysicalOlapScan[t2]                                               |
  | ------------PhysicalDistribute[DistributionSpecReplicated]                       |
  | --------------PhysicalProject                                                    |
  | ----------------PhysicalOlapScan[t1]                                             |
  |                                                                                  |
  | Hint log:                                                                        |
  | Used: ORDERED                                                                    |
  | UnUsed:                                                                          |
  | SyntaxError:                                                                     |
  +----------------------------------------------------------------------------------+
  16 rows in set (0.01 sec)
  ```
- the Explain shape plan will display inside distribute operator related information, including DistributionSpecReplicated said the operator will be corresponding data into a copy of all be node, DistributionSpecGather indicates that data is gathered to fe nodes, and DistributionSpecHash indicates that data is dispersed to different be nodes according to a specific hashKey and algorithm.
# To be supported
- leadingHint indicates that the current query cannot be mixed with the subquery after it is promoted. A hint is required to control whether the subquery can be unnested

- A new distributeHint is needed for better and more complete control of the distribute operator

- Use leadingHint and distributeHint together to determine the shape of a join
