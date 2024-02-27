---
{
    "title": "WINDOW_FUNCTION_CUME_DIST",
    "language": "zh-CN"
}
---

<!--  Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License. -->

## WINDOW FUNCTION CUME_DIST
### description

CUME_DIST (Cumulative Distribution) 是一种窗口函数，它常用于计算当前行值在排序后结果集中的相对排名。它返回的是当前行值在结果集中的百分比排名，即在排序后的结果中小于或等于当前行值的行数与结果集总行数的比例。

```sql
CUME_DIST() OVER(partition_by_clause order_by_clause)
```

### example
假设有一个表格 sales 包含销售数据，其中包括销售员姓名 (sales_person)、销售额 (sales_amount) 和销售日期 (sales_date)。我们想要计算每个销售员在每个销售日期的销售额占当日总销售额的累积百分比。

```sql
SELECT 
    sales_person,
    sales_date,
    sales_amount,
    CUME_DIST() OVER (PARTITION BY sales_date ORDER BY sales_amount ASC) AS cumulative_sales_percentage
FROM 
    sales;
```

假设表格 sales 中的数据如下：
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
执行上述 SQL 查询后，结果将显示每个销售员在每个销售日期的销售额以及其在该销售日期的累积百分比排名。
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
在这个例子中，CUME_DIST() 函数根据每个销售日期对销售额进行排序，然后计算每个销售员在该销售日期的销售额占当日总销售额的累积百分比。由于我们使用了 PARTITION BY sales_date，所以计算是在每个销售日期内进行的，销售员在不同日期的销售额被分别计算。

### keywords

    WINDOW,FUNCTION,CUME_DIST

