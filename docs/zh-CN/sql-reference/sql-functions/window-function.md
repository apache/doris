---
{
    "title": "窗口函数",
    "language": "zh-CN"
}
---

<!--  Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License. -->

# Doris 窗口函数使用

## 窗口函数介绍

分析函数是一类特殊的内置函数。和聚合函数类似，分析函数也是对于多个输入行做计算得到一个数据值。不同的是，分析函数是在一个特定的窗口内对输入数据做处理，而不是按照 group by 来分组计算。每个窗口内的数据可以用 over() 从句进行排序和分组。分析函数会对结果集的每一行计算出一个单独的值，而不是每个 group by 分组计算一个值。这种灵活的方式允许用户在 select 从句中增加额外的列，给用户提供了更多的机会来对结果集进行重新组织和过滤。分析函数只能出现在 select 列表和最外层的 order by 从句中。在查询过程中，分析函数会在最后生效，就是说，在执行完 join，where 和 group by 等操作之后再执行。分析函数在金融和科学计算领域经常被使用到，用来分析趋势、计算离群值以及对大量数据进行分桶分析等。

分析函数的语法：

```sql
function(args) OVER(partition_by_clause order_by_clause [window_clause])    
partition_by_clause ::= PARTITION BY expr [, expr ...]    
order_by_clause ::= ORDER BY expr [ASC | DESC] [, expr [ASC | DESC] ...]
```

### Function

目前支持的 Function 包括 AVG(), COUNT(), DENSE_RANK(), FIRST_VALUE(), LAG(), LAST_VALUE(), LEAD(), MAX(), MIN(), RANK(), ROW_NUMBER() 和 SUM()。

### PARTITION BY从句

Partition By 从句和 Group By 类似。它把输入行按照指定的一列或多列分组，相同值的行会被分到一组。

### ORDER BY从句

Order By从句和外层的Order By基本一致。它定义了输入行的排列顺序，如果指定了 Partition By，则 Order By 定义了每个 Partition 分组内的顺序。与外层 Order By 的唯一不同点是，OVER 从句中的 Order By n（n是正整数）相当于不做任何操作，而外层的 Order By n表示按照第n列排序。

举例:

这个例子展示了在select列表中增加一个id列，它的值是1，2，3等等，顺序按照events表中的date_and_time列排序。

```sql
SELECT   
row_number() OVER (ORDER BY date_and_time) AS id,   
c1, c2, c3, c4   
FROM events;
```

### Window从句

Window 从句用来为分析函数指定一个运算范围，以当前行为准，前后若干行作为分析函数运算的对象。Window 从句支持的方法有：AVG(), COUNT(), FIRST_VALUE(), LAST_VALUE() 和 SUM()。对于 MAX() 和 MIN(), window 从句可以指定开始范围 UNBOUNDED PRECEDING

语法:

```sql
ROWS BETWEEN [ { m | UNBOUNDED } PRECEDING | CURRENT ROW] [ AND [CURRENT ROW | { UNBOUNDED | n } FOLLOWING] ]
```

### 举例：

假设我们有如下的股票数据，股票代码是 JDR，closing price 是每天的收盘价。

```sql
create table stock_ticker (stock_symbol string, closing_price decimal(8,2), closing_date timestamp);    
...load some data...    
select * from stock_ticker order by stock_symbol, closing_date
 | stock_symbol | closing_price | closing_date        |
 |--------------|---------------|---------------------|
 | JDR          | 12.86         | 2014-10-02 00:00:00 |
 | JDR          | 12.89         | 2014-10-03 00:00:00 |
 | JDR          | 12.94         | 2014-10-04 00:00:00 |
 | JDR          | 12.55         | 2014-10-05 00:00:00 |
 | JDR          | 14.03         | 2014-10-06 00:00:00 |
 | JDR          | 14.75         | 2014-10-07 00:00:00 |
 | JDR          | 13.98         | 2014-10-08 00:00:00 |
```

这个查询使用分析函数产生 moving_average 这一列，它的值是3天的股票均价，即前一天、当前以及后一天三天的均价。第一天没有前一天的值，最后一天没有后一天的值，所以这两行只计算了两天的均值。这里 Partition By 没有起到作用，因为所有的数据都是 JDR 的数据，但如果还有其他股票信息，Partition By 会保证分析函数值作用在本 Partition 之内。

```sql
select stock_symbol, closing_date, closing_price,    
avg(closing_price) over (partition by stock_symbol order by closing_date    
rows between 1 preceding and 1 following) as moving_average    
from stock_ticker;
 | stock_symbol | closing_date        | closing_price | moving_average |
 |--------------|---------------------|---------------|----------------|
 | JDR          | 2014-10-02 00:00:00 | 12.86         | 12.87          |
 | JDR          | 2014-10-03 00:00:00 | 12.89         | 12.89          |
 | JDR          | 2014-10-04 00:00:00 | 12.94         | 12.79          |
 | JDR          | 2014-10-05 00:00:00 | 12.55         | 13.17          |
 | JDR          | 2014-10-06 00:00:00 | 14.03         | 13.77          |
 | JDR          | 2014-10-07 00:00:00 | 14.75         | 14.25          |
 | JDR          | 2014-10-08 00:00:00 | 13.98         | 14.36          |
```

## Function使用举例

本节介绍 Doris 中可以用作分析函数的方法。

### AVG()

语法：

```sql
AVG([DISTINCT | ALL] *expression*) [OVER (*analytic_clause*)]
```

举例：

计算当前行和它前后各一行数据的x平均值

```sql
select x, property,    
avg(x) over    
(   
partition by property    
order by x    
rows between 1 preceding and 1 following    
) as 'moving average'    
from int_t where property in ('odd','even');
 | x  | property | moving average |
 |----|----------|----------------|
 | 2  | even     | 3              |
 | 4  | even     | 4              |
 | 6  | even     | 6              |
 | 8  | even     | 8              |
 | 10 | even     | 9              |
 | 1  | odd      | 2              |
 | 3  | odd      | 3              |
 | 5  | odd      | 5              |
 | 7  | odd      | 7              |
 | 9  | odd      | 8              |
```

### COUNT()

语法：

```sql
COUNT([DISTINCT | ALL] expression) [OVER (analytic_clause)]
```

举例：

计算从当前行到第一行x出现的次数。

```sql
select x, property,   
count(x) over   
(   
partition by property    
order by x    
rows between unbounded preceding and current row    
) as 'cumulative total'    
from int_t where property in ('odd','even');
 | x  | property | cumulative count |
 |----|----------|------------------|
 | 2  | even     | 1                |
 | 4  | even     | 2                |
 | 6  | even     | 3                |
 | 8  | even     | 4                |
 | 10 | even     | 5                |
 | 1  | odd      | 1                |
 | 3  | odd      | 2                |
 | 5  | odd      | 3                |
 | 7  | odd      | 4                |
 | 9  | odd      | 5                |
```

### DENSE_RANK()

DENSE_RANK() 函数用来表示排名，与RANK()不同的是，DENSE_RANK() 不会出现空缺数字。比如，如果出现了两个并列的1，DENSE_RANK() 的第三个数仍然是2，而RANK()的第三个数是3。

语法：

```sql
DENSE_RANK() OVER(partition_by_clause order_by_clause)
```

举例：

下例展示了按照 property 列分组对x列排名：

```sql
 select x, y, dense_rank() over(partition by x order by y) as rank from int_t;
 | x  | y    | rank     |
 |----|------|----------|
 | 1  | 1    | 1        |
 | 1  | 2    | 2        |
 | 1  | 2    | 2        |
 | 2  | 1    | 1        |
 | 2  | 2    | 2        |
 | 2  | 3    | 3        |
 | 3  | 1    | 1        |
 | 3  | 1    | 1        |
 | 3  | 2    | 2        |
```

### FIRST_VALUE()

FIRST_VALUE() 返回窗口范围内的第一个值。

语法：

```sql
FIRST_VALUE(expr) OVER(partition_by_clause order_by_clause [window_clause])
```

举例：

我们有如下数据

```sql
 select name, country, greeting from mail_merge;
 | name    | country | greeting     |
 |---------|---------|--------------|
 | Pete    | USA     | Hello        |
 | John    | USA     | Hi           |
 | Boris   | Germany | Guten tag    |
 | Michael | Germany | Guten morgen |
 | Bjorn   | Sweden  | Hej          |
 | Mats    | Sweden  | Tja          |
```

使用 FIRST_VALUE()，根据 country 分组，返回每个分组中第一个 greeting 的值：

```sql
select country, name,    
first_value(greeting)    
over (partition by country order by name, greeting) as greeting from mail_merge;
| country | name    | greeting  |
|---------|---------|-----------|
| Germany | Boris   | Guten tag |
| Germany | Michael | Guten tag |
| Sweden  | Bjorn   | Hej       |
| Sweden  | Mats    | Hej       |
| USA     | John    | Hi        |
| USA     | Pete    | Hi        |
```

### LAG()

LAG() 方法用来计算当前行向前数若干行的值。

语法：

```sql
LAG (expr, offset, default) OVER (partition_by_clause order_by_clause)
```

举例：

计算前一天的收盘价

```sql
select stock_symbol, closing_date, closing_price,    
lag(closing_price,1, 0) over (partition by stock_symbol order by closing_date) as "yesterday closing"   
from stock_ticker   
order by closing_date;
| stock_symbol | closing_date        | closing_price | yesterday closing |
|--------------|---------------------|---------------|-------------------|
| JDR          | 2014-09-13 00:00:00 | 12.86         | 0                 |
| JDR          | 2014-09-14 00:00:00 | 12.89         | 12.86             |
| JDR          | 2014-09-15 00:00:00 | 12.94         | 12.89             |
| JDR          | 2014-09-16 00:00:00 | 12.55         | 12.94             |
| JDR          | 2014-09-17 00:00:00 | 14.03         | 12.55             |
| JDR          | 2014-09-18 00:00:00 | 14.75         | 14.03             |
| JDR          | 2014-09-19 00:00:00 | 13.98         | 14.75             |
```

### LAST_VALUE()

LAST_VALUE() 返回窗口范围内的最后一个值。与 FIRST_VALUE() 相反。

语法：

```sql
LAST_VALUE(expr) OVER(partition_by_clause order_by_clause [window_clause])
```

使用FIRST_VALUE()举例中的数据：

```sql
select country, name,    
last_value(greeting)   
over (partition by country order by name, greeting) as greeting   
from mail_merge;
| country | name    | greeting     |
|---------|---------|--------------|
| Germany | Boris   | Guten morgen |
| Germany | Michael | Guten morgen |
| Sweden  | Bjorn   | Tja          |
| Sweden  | Mats    | Tja          |
| USA     | John    | Hello        |
| USA     | Pete    | Hello        |
```

### LEAD()

LEAD() 方法用来计算当前行向后数若干行的值。

语法：

```sql
LEAD (expr, offset, default]) OVER (partition_by_clause order_by_clause)
```

举例：

计算第二天的收盘价对比当天收盘价的走势，即第二天收盘价比当天高还是低。

```sql
select stock_symbol, closing_date, closing_price,    
case   
(lead(closing_price,1, 0)   
over (partition by stock_symbol order by closing_date)-closing_price) > 0   
when true then "higher"   
when false then "flat or lower"    
end as "trending"   
from stock_ticker    
order by closing_date;
| stock_symbol | closing_date        | closing_price | trending      |
|--------------|---------------------|---------------|---------------|
| JDR          | 2014-09-13 00:00:00 | 12.86         | higher        |
| JDR          | 2014-09-14 00:00:00 | 12.89         | higher        |
| JDR          | 2014-09-15 00:00:00 | 12.94         | flat or lower |
| JDR          | 2014-09-16 00:00:00 | 12.55         | higher        |
| JDR          | 2014-09-17 00:00:00 | 14.03         | higher        |
| JDR          | 2014-09-18 00:00:00 | 14.75         | flat or lower |
| JDR          | 2014-09-19 00:00:00 | 13.98         | flat or lower |
```

### MAX()

语法：

```sql
MAX([DISTINCT | ALL] expression) [OVER (analytic_clause)]
```

举例：

计算从第一行到当前行之后一行的最大值

```sql
select x, property,   
max(x) over    
(   
order by property, x    
rows between unbounded preceding and 1 following    
) as 'local maximum'    
from int_t where property in ('prime','square');
| x | property | local maximum |
|---|----------|---------------|
| 2 | prime    | 3             |
| 3 | prime    | 5             |
| 5 | prime    | 7             |
| 7 | prime    | 7             |
| 1 | square   | 7             |
| 4 | square   | 9             |
| 9 | square   | 9             |
```

### MIN()

语法：

```sql
MIN([DISTINCT | ALL] expression) [OVER (analytic_clause)]
```

举例：

计算从第一行到当前行之后一行的最小值

```sql
select x, property,   
min(x) over    
(    
order by property, x desc    
rows between unbounded preceding and 1 following   
) as 'local minimum'   
from int_t where property in ('prime','square');
| x | property | local minimum |
|---|----------|---------------|
| 7 | prime    | 5             |
| 5 | prime    | 3             |
| 3 | prime    | 2             |
| 2 | prime    | 2             |
| 9 | square   | 2             |
| 4 | square   | 1             |
| 1 | square   | 1             |
```

### RANK()

RANK() 函数用来表示排名，与 DENSE_RANK() 不同的是，RANK() 会出现空缺数字。比如，如果出现了两个并列的1， RANK() 的第三个数就是3，而不是2。

语法：

```sql
RANK() OVER(partition_by_clause order_by_clause)
```

举例：

根据 x 进行排名

```sql
select x, y, rank() over(partition by x order by y) as rank from int_t;
| x  | y    | rank     |
|----|------|----------|
| 1  | 1    | 1        |
| 1  | 2    | 2        |
| 1  | 2    | 2        |
| 2  | 1    | 1        |
| 2  | 2    | 2        |
| 2  | 3    | 3        |
| 3  | 1    | 1        |
| 3  | 1    | 1        |
| 3  | 2    | 3        |
```

### ROW_NUMBER()

为每个 Partition 的每一行返回一个从1开始连续递增的整数。与 RANK() 和 DENSE_RANK() 不同的是，ROW_NUMBER() 返回的值不会重复也不会出现空缺，是连续递增的。

语法：

```sql
ROW_NUMBER() OVER(partition_by_clause order_by_clause)
```

举例：

```sql
select x, y, row_number() over(partition by x order by y) as rank from int_t;
| x | y    | rank     |
|---|------|----------|
| 1 | 1    | 1        |
| 1 | 2    | 2        |
| 1 | 2    | 3        |
| 2 | 1    | 1        |
| 2 | 2    | 2        |
| 2 | 3    | 3        |
| 3 | 1    | 1        |
| 3 | 1    | 2        |
| 3 | 2    | 3        |
```

### SUM()

语法：

```sql
SUM([DISTINCT | ALL] expression) [OVER (analytic_clause)]
```

举例：

按照 property 进行分组，在组内计算当前行以及前后各一行的x列的和。

```sql
select x, property,   
sum(x) over    
(   
partition by property   
order by x   
rows between 1 preceding and 1 following    
) as 'moving total'    
from int_t where property in ('odd','even');
| x  | property | moving total |
|----|----------|--------------|
| 2  | even     | 6            |
| 4  | even     | 12           |
| 6  | even     | 18           |
| 8  | even     | 24           |
| 10 | even     | 18           |
| 1  | odd      | 4            |
| 3  | odd      | 9            |
| 5  | odd      | 15           |
| 7  | odd      | 21           |
| 9  | odd      | 16           |
```

