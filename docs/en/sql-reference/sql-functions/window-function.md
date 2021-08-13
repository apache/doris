---
{
    "title": "window function",
    "language": "en"
}
---

<!--  Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License. -->

# Doris Window function usage

## Window function introduction

Analysis functions are a special kind of built-in functions. Similar to the aggregation function, the analysis function also calculates a data value for multiple input rows. The difference is that the analysis function processes the input data in a specific window instead of grouping calculations according to group by. The data in each window can be sorted and grouped using the over() clause. The analysis function calculates a separate value for each row of the result set, instead of calculating a value for each group by group. This flexible way allows users to add additional columns in the select clause, giving users more opportunities to reorganize and filter the result set. Analysis functions can only appear in the select list and the outermost order by clause. In the query process, the analysis function will take effect at the end, that is, it will be executed after the join, where and group  by operations are completed. Analytical functions are often used in the fields of finance and scientific computing to analyze trends, calculate outliers, and perform bucket analysis on large amounts of data.

The syntax of the analysis function:

```sql
function(args) OVER(partition_by_clause order_by_clause [window_clause])    
partition_by_clause ::= PARTITION BY expr [, expr ...]    
order_by_clause ::= ORDER BY expr [ASC | DESC] [, expr [ASC | DESC] ...]
```

### Function

Currently supported functions include AVG(), COUNT(), DENSE_RANK(), FIRST_VALUE(), LAG(), LAST_VALUE(), LEAD(), MAX(), MIN(), RANK(), ROW_NUMBER() and SUM ().

### Partition By clause

The Partition By clause is similar to Group By. It groups the input rows according to the specified one or more columns, and rows with the same value will be grouped into a group.

### Order By clause

The Order By clause is basically the same as the outer Order By. It defines the order of the input rows. If Partition By is specified, Order By defines the order within each Partition group. The only difference with the outer Order By is that the Order By n (n is a positive integer) in the OVER clause is equivalent to doing nothing, while the outer Order By n means sorting according to the nth column.

For example:

This example shows the addition of an id column to the select list, its value is 1, 2, 3, etc., in order according to the date_and_time column in the events table.

```sql
SELECT   
row_number() OVER (ORDER BY date_and_time) AS id,   
c1, c2, c3, c4   
FROM events;
```

### Window clause

The Window clause is used to specify an operation range for the analysis function, based on the current behavior, and several lines before and after the analysis function as the object of operation. The methods supported by the Window clause are: AVG(), COUNT(), FIRST_VALUE(), LAST_VALUE() and SUM(). For MAX() and MIN(), the window clause can specify the start range UNBOUNDED PRECEDING

grammar:

```sql
ROWS BETWEEN [ { m | UNBOUNDED } PRECEDING | CURRENT ROW] [ AND [CURRENT ROW | { UNBOUNDED | n } FOLLOWING] ]
```

### Example:

Suppose we have the following stock data, the stock code is JDR, and the closing price is the daily closing price.

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

This query uses an analytical function to generate the moving_average column, and its value is the average price of stocks in 3 days, that is, the average price of the previous day, the current day, and the next day. The first day does not have the value of the previous day, and the last day does not have the value of the next day, so these two rows only calculate the average of the two days. Here Partition By does not play a role, because all the data is JDR data, but if there is other stock information, Partition By will ensure that the analysis function value is applied to this Partition.

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

## Function example

This section introduces the methods that can be used as analysis functions in Doris.

### AVG()

grammar：

```sql
AVG([DISTINCT | ALL] *expression*) [OVER (*analytic_clause*)]
```

For example:

Calculate the x average value of the current row and each row of data before and after it.

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

grammar：

```sql
COUNT([DISTINCT | ALL] expression) [OVER (analytic_clause)]
```

For example:

Count the number of occurrences of x from the current line to the first line.

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

The DENSE_RANK() function is used to indicate the ranking. Unlike RANK(), DENSE_RANK() does not have vacant numbers. For example, if there are two parallel ones, the third number of DENSE_RANK() is still 2, and the third number of RANK() is 3.

grammar：

```sql
DENSE_RANK() OVER(partition_by_clause order_by_clause)
```

For example:

The following example shows the ranking of the x column grouped by the property column:

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

FIRST_VALUE() returns the first value in the window range.

grammar：

```sql
FIRST_VALUE(expr) OVER(partition_by_clause order_by_clause [window_clause])
```

For example:

We have the following data

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

Use FIRST_VALUE() to group by country and return the value of the first greeting in each group：

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

The LAG() method is used to calculate the value of several lines forward from the current line.

grammar：

```sql
LAG (expr, offset, default) OVER (partition_by_clause order_by_clause)
```

For example:

Calculate the closing price of the previous day

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

LAST_VALUE() returns the last value in the window range. Contrary to FIRST_VALUE().

grammar：

```sql
LAST_VALUE(expr) OVER(partition_by_clause order_by_clause [window_clause])
```

Use the data in the FIRST_VALUE() example:

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

The LEAD() method is used to calculate the value of several rows from the current row.

grammar：

```sql
LEAD (expr, offset, default]) OVER (partition_by_clause order_by_clause)
```

For example:

Calculate the trend of the closing price of the next day compared to the closing price of the day, that is, whether the closing price of the next day is higher or lower than that of the day.

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

grammar：

```sql
MAX([DISTINCT | ALL] expression) [OVER (analytic_clause)]
```

For example:

Calculate the maximum value from the first line to the line after the current line

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

grammar：

```sql
MIN([DISTINCT | ALL] expression) [OVER (analytic_clause)]
```

For example:

Calculate the minimum value from the first line to the line after the current line

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

The RANK() function is used to indicate ranking. Unlike DENSE_RANK(), RANK() will have vacant numbers. For example, if there are two parallel 1s, the third number in RANK() is 3, not 2.

grammar：

```sql
RANK() OVER(partition_by_clause order_by_clause)
```

For example:

Rank according to x

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

For each row of each Partition, an integer that starts from 1 and increases continuously is returned. Unlike RANK() and DENSE_RANK(), the value returned by ROW_NUMBER() will not be repeated or vacant, and is continuously increasing.

grammar：

```sql
ROW_NUMBER() OVER(partition_by_clause order_by_clause)
```

For example:

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

grammar：

```sql
SUM([DISTINCT | ALL] expression) [OVER (analytic_clause)]
```

For example:

Group according to property, and calculate the sum of the x column of the current row and each row before and after in the group.

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

