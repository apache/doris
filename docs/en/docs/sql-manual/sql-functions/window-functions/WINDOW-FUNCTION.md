---
{
    "title": "Window Functions Overview",
    "language": "en"
}
---

<!--  Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License. -->

## WINDOW FUNCTION
### description

Analytical functions(windown function) are a special class of built-in functions. Similar to aggregate functions, analytic functions also perform calculations on multiple input rows to obtain a data value. The difference is that the analytic function processes the input data within a specific window, rather than grouping calculations by group by. The data within each window can be sorted and grouped using the over() clause. The analytic function computes a single value for each row of the result set, rather than one value per group by grouping. This flexible approach allows the user to add additional columns to the select clause, giving the user more opportunities to reorganize and filter the result set. Analytic functions can only appear in select lists and in the outermost order by clause. During the query process, the analytical function will take effect at the end, that is, after the join, where and group by operations are performed. Analytical functions are often used in financial and scientific computing to analyze trends, calculate outliers, and perform bucket analysis on large amounts of data.

The syntax of the analytic function:

```sql
function(args) OVER(partition_by_clause order_by_clause [window_clause])    
partition_by_clause ::= PARTITION BY expr [, expr ...]    
order_by_clause ::= ORDER BY expr [ASC | DESC] [, expr [ASC | DESC] ...]
```

#### Function

Support Functions: AVG(), COUNT(), DENSE_RANK(), FIRST_VALUE(), LAG(), LAST_VALUE(), LEAD(), MAX(), MIN(), RANK(), ROW_NUMBER(), SUM()

#### PARTITION BY clause

The Partition By clause is similar to Group By. It groups the input rows according to the specified column or columns, and rows with the same value will be grouped together.

#### ORDER BY clause

The Order By clause is basically the same as the outer Order By. It defines the order in which the input rows are sorted, and if Partition By is specified, Order By defines the order within each Partition grouping. The only difference from the outer Order By is that the Order By n (n is a positive integer) in the OVER clause is equivalent to doing nothing, while the outer Order By n means sorting according to the nth column.

Example:

This example shows adding an id column to the select list with values 1, 2, 3, etc., sorted by the date_and_time column in the events table.

```sql
SELECT   
row_number() OVER (ORDER BY date_and_time) AS id,   
c1, c2, c3, c4   
FROM events;
```

#### Window clause

The Window clause is used to specify an operation range for the analytical function, the current row is the criterion, and several rows before and after are used as the object of the analytical function operation. The methods supported by the Window clause are: AVG(), COUNT(), FIRST_VALUE(), LAST_VALUE() and SUM(). For MAX() and MIN(), the window clause can specify the starting range UNBOUNDED PRECEDING

syntax:

```sql
ROWS BETWEEN [ { m | UNBOUNDED } PRECEDING | CURRENT ROW] [ AND [CURRENT ROW | { UNBOUNDED | n } FOLLOWING] ]
```

### example

Suppose we have the following stock data, the stock symbol is JDR, and the closing price is the closing price of each day.

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

This query uses the analytic function to generate the column moving_average, whose value is the 3-day average price of the stock, that is, the three-day average price of the previous day, the current day, and the next day. The first day has no value for the previous day, and the last day does not have the value for the next day, so these two lines only calculate the average of the two days. Partition By does not play a role here, because all the data are JDR data, but if there is other stock information, Partition By will ensure that the analysis function value acts within this Partition.

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

### keywords

    WINDOW,FUNCTION
