# join hint 使用文档
# 背景
在数据库中，"Hint" 是一种用于指导查询优化器执行计划的指令。通过在SQL语句中嵌入Hint，可以影响优化器的决策，以选中期望的执行路径。以下是一个使用Hint的背景示例：
假设有一个包含大量数据的表，而你知道在某些特定情况下，在一个查询中，表的连接顺序可能会影响查询性能。Leading Hint允许你指定希望优化器遵循的表连接的顺序。
例如，考虑以下SQL查询：
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

在上述例子里面，在执行效率不理想的时候，我们希望调整下join顺序而不改变原始sql以免影响到用户原始场景且能达到调优的目的。我们可以使用leading任意改变tableA和tableB的join顺序。例如可以写成：
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
在这个例子中，使用了 /*+ leading(t2 t1) */ 这个Hint。这个Hint告诉优化器在执行计划中使用指定表（t2）作为驱动表，并置于(t1)之前。
本文主要阐述如何在Doris里面使用join相关的hint：leading hint、ordered hint 和 distribute hint
# Leading hint使用说明
Leading Hint 用于指导优化器确定查询计划的连接顺序。在一个查询中，表的连接顺序可能会影响查询性能。Leading Hint允许你指定希望优化器遵循的表连接的顺序。
在doris里面，其语法为 /*+LEADING( tablespec [ tablespec ]...  ) */,leading由"/*+"和"*/"包围并置于select语句里面 select的正后方。注意，leading 后方的 '/' 和selectlist需要隔开至少一个分割符例如空格。至少需要写两个以上的表才认为这个leadinghint是合理的。且任意的join里面可以用大括号括号起来，来显式地指定joinTree的形状。例：
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
- 当leadinghint不生效的时候会走正常的流程生成计划，explain会显示使用的hint是否生效，主要分三种来显示：
    - Used：leading hint正常生效
    - Unused： 这里不支持的情况包含leading指定的join order与原sql不等价或本版本暂不支持特性（详见限制）
    - SyntaxError： 指leading hint语法错误，如找不到对应的表等
- leading hint语法默认造出来左深树，例：select /*+ leading(t1 t2 t3) */ * from t1 join t2 on...默认指定出来
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

- 同时允许使用大括号指定join树形状。例：/*+ leading(t1 {t2 t3}) */
  join
  /    \
  t1    join
  /    \
  t2    t3

```sql
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

- 当有view作为别名参与joinReorder的时候可以指定对应的view作为leading的参数。例：
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
## 基本用例
  （注意这里列命名和表命名相关，例：只有t1中有c1字段，后续例子为了简化会将 t1.c1 直接写成 c1）
```sql
CREATE DATABASE testleading;
USE testleading;

create table t1 (c1 int, c11 int) distributed by hash(c1) buckets 3 properties('replication_num' = '1');
create table t2 (c2 int, c22 int) distributed by hash(c2) buckets 3 properties('replication_num' = '1');
create table t3 (c3 int, c33 int) distributed by hash(c3) buckets 3 properties('replication_num' = '1');
create table t4 (c4 int, c44 int) distributed by hash(c4) buckets 3 properties('replication_num' = '1');

```  
举个简单的例子，当我们需要交换t1和t2的join顺序的时候只需要在前面加上leading(t2 t1)即可，explain的时候会
显示是否用上了这个hint。
原始plan
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
hint 效果展示
（Used unused）
若leading hint有语法错误，explain的时候会在syntax error里面显示相应的信息，但是计划能照常生成，只不过没有使用leading而已
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
## 扩展场景
### 左深树
当我们不使用任何括号的情况下leading会默认生成左深树
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
### 右深树
当我们想将计划的形状做成右深树或者bushy树或者zigzag树的时候，只需要加上大括号来限制plan的形状即可，不需要像oracle一样用swap从左深树一步步调整。
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
### Bushy 树
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
### zig-zag 树
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
当遇到非inner-join的时候，例如Outer join或者semi/anti join的时候，leading hint会根据原始sql语义自动推导各个join的join方式。若遇到与原始sql语义不同的leading hint或者生成不了的情况则会放到unused里面，但是不影响计划正常流程的生成。
下面是不能交换的例子：  
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
下面是一些可以交换的例子和不能交换的例子,读者可自行验证
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
遇到别名的情况，可以将别名作为一个完整的子树进行指定，子树里面的joinOrder由文本序生成。
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
## 与ordered混合使用
当与ordered hint混合使用的时候以ordered hint为主，即ordered hint生效优先级高于leading hint。例：
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
## 使用限制
- 当前版本只支持使用一个leadingHint。若和子查询同时使用leadinghint的话则查询会报错。例（这个例子explain会报错，但是会走正常的路径生成计划）：
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
 # OrderedHint 使用说明
- 使用ordered hint会让join tree的形状固定下来，按照文本序来显示
- 语法为 /*+ ORDERED */,leading由"/*+"和"*/"包围并置于select语句里面 select的正后方，例：
  explain shape plan select /*+ ORDERED */ t1.c1 from t2 join t1 on t1.c1 = t2.c2 join t3 on c2 = c3;
  join
  /    \
  join    t3
  /    \
  t2    t1

```sql
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
- 当ordered hint和leading hint同时使用时以ordered hint为准，leading hint会失效
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
# DistributeHint 使用说明
- 目前只能指定右表的distribute Type 而且只有[shuffle] 和 [broadcast]两种，写在join右表前面且允许中括号和/*+ */两种写法
- 目前能使用任意个DistributeHint
- 当遇到无法正确生成计划的 DistributeHint，没有显示，按最大努力生效，最后以explain显示的distribute方式为主
- 当前版本暂不与leading混用，且当distribute指定的表位于join右边才可生效。
- 多与ordered混用，利用文本序把join顺序固定下来，然后再指定相应的join里面我们预期使用什么样的distribute方式。例：
  使用前：
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

  使用后：
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
- Explain shape plan里面会显示distribute算子相关的信息，其中DistributionSpecReplicated表示该算子将对应的数据变成所有be节点复制一份，DistributionSpecGather表示将数据gather到fe节点，DistributionSpecHash表示将数据按照特定的hashKey以及算法打散到不同的be节点。
# 待支持
- leadingHint待支持子查询解嵌套指定，当前和子查询提升以后不能混用，需要有hint来控制是否可以解嵌套
- 需要新的distributeHint来更好且更全面地控制distribute算子
- 混合使用leadingHint与distributeHint来共同确定join的形状
