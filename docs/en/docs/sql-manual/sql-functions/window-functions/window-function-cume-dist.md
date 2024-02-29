---
{
    "title": "WINDOW_FUNCTION_CUME_DIST",
    "language": "en"
}
---

<!--  Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License. -->

## WINDOW FUNCTION CUME_DIST
### description

CUME_DIST (Cumulative Distribution) is a window function commonly used to calculate the relative ranking of the current row value within a sorted result set. It returns the percentage ranking of the current row value in the result set, i.e., the ratio of the number of rows less than or equal to the current row value to the total number of rows in the result set after sorting.

```sql
CUME_DIST() OVER(partition_by_clause order_by_clause)
```

### example
Suppose there is a table named sales containing sales data, including salesperson name (sales_person), sales amount (sales_amount), and sales date (sales_date). We want to calculate the cumulative percentage of sales amount for each salesperson on each sales date compared to the total sales amount for that day.
```sql
SELECT 
    sales_person,
    sales_date,
    sales_amount,
    CUME_DIST() OVER (PARTITION BY sales_date ORDER BY sales_amount ASC) AS cumulative_sales_percentage
FROM 
    sales;
```

Suppose the data in the sales table is as follows:

```sql
+------+--------------+------------+--------------+
| id   | sales_person | sales_date | sales_amount |
+------+--------------+------------+--------------+
|    1 | Alice        | 2024-02-01 |         2000 |
|    2 | Bob          | 2024-02-01 |         1500 |
|    3 | Alice        | 2024-02-02 |         1800 |
|    4 | Bob          | 2024-02-02 |         1200 |
|    5 | Alice        | 2024-02-03 |         2200 |
|    6 | Bob          | 2024-02-03 |         1900 |
|    7 | Tom          | 2024-02-03 |         2000 |
|    8 | Jerry        | 2024-02-03 |         2000 |
+------+--------------+------------+--------------+
```

After executing the above SQL query, the result will display the sales amount for each salesperson on each sales date and their cumulative percentage ranking for that sales date.
```sql
+--------------+------------+--------------+-----------------------------+
| sales_person | sales_date | sales_amount | cumulative_sales_percentage |
+--------------+------------+--------------+-----------------------------+
| Bob          | 2024-02-01 |         1500 |                         0.5 |
| Alice        | 2024-02-01 |         2000 |                           1 |
| Bob          | 2024-02-02 |         1200 |                         0.5 |
| Alice        | 2024-02-02 |         1800 |                           1 |
| Bob          | 2024-02-03 |         1900 |                        0.25 |
| Tom          | 2024-02-03 |         2000 |                        0.75 |
| Jerry        | 2024-02-03 |         2000 |                        0.75 |
| Alice        | 2024-02-03 |         2200 |                           1 |
+--------------+------------+--------------+-----------------------------+
```
In this example, the CUME_DIST() function sorts the sales amount for each sales date and then calculates the cumulative percentage of sales amount for each salesperson on that date compared to the total sales amount for that day. Since we use PARTITION BY sales_date, the calculation is done within each sales date, and the sales amount for salespersons on different dates is calculated separately.
### keywords

    WINDOW,FUNCTION,CUME_DIST

