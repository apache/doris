---
{
    "title": "GROUPING_ID",
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

## GROUPING_ID

### Name

GROUPING_ID

### Description

这是一个用来计算分组级别的函数。当 SQL 语句中使用了 `GROUP BY` 子句时，`GROUPING_ID` 函数可以在 `SELECT <select> list`、`HAVING` 或 `ORDER BY` 子句中使用。

#### Syntax

```sql
GROUPING_ID ( <column_expression>[ ,...n ] )
```

#### Arguments

`<column_expression>`

是在 `GROUP BY` 子句中包含的列或表达式。

#### Return Type

BIGINT

#### Remarks

GROUPING_ID 函数的入参 `<column_expression>` 必须和 `GROUP BY` 子句的表达式一致。比如说，如果你按 `user_id` 进行 `GROUP BY`，那么你的 GROUPING_ID 函数应该这么写：`GROUPING_ID (user_id)`。再比如说，你按 `name` 进行 `GROUP BY`，那么函数应该这么写：`GROUPING_ID (name)`。

#### Comparing GROUPING_ID() to GROUPING()

`GROUPING_ID(<column_expression> [ ,...n ])` 的计算规则为，对于输入的字段（或表达式）列表，分别对每个字段（或表达式）进行 `GROUPING(<column_expression>)` 运算，得到的结果组成一个 01 串。这个 01 串实际上是二进制数，GROUPING_ID 函数会将其转化为十进制数返回。比如说，以 `SELECT a, b, c, SUM(d), GROUPING_ID(a,b,c) FROM T GROUP BY <group by list>` 语句为例，下面展示了 GROUPING_ID() 函数的输入和输出。

| Columns aggregated | GROUPING_ID (a, b, c) input = GROUPING(a) + GROUPING(b) + GROUPING(c) | GROUPING_ID () output |
| ------------------ | ------------------------------------------------------------ | --------------------- |
| `a`                | `100`                                                        | `4`                   |
| `b`                | `010`                                                        | `2`                   |
| `c`                | `001`                                                        | `1`                   |
| `ab`               | `110`                                                        | `6`                   |
| `ac`               | `101`                                                        | `5`                   |
| `bc`               | `011`                                                        | `3`                   |
| `abc`              | `111`                                                        | `7`                   |

#### Technical Definition of GROUPING_ID()

GROUPING_ID 函数的入参必须是 GROUP BY 子句中的字段（或字段表达式）。GROUPING_ID() 函数返回一个整数位图，位图中的每一位均与 GROUP BY 子句中的字段（或字段表达式）一一对应，位图中的最低位代表第 N 个参数，第二低位代表第 N-1 个参数，以此类推。当某一位被置为 1 时，表示其对应的列不参与分组聚合。

#### GROUPING_ID() Equivalents

对于多个字段（或字段表达式）进行分组查询时，以下两个声明是等价的：

声明 A：

```sql
SELECT GROUPING_ID(A,B)  
FROM T   
GROUP BY CUBE(A,B)
```

声明 B：

```sql
SELECT 3 FROM T GROUP BY ()  
UNION ALL  
SELECT 1 FROM T GROUP BY A  
UNION ALL  
SELECT 2 FROM T GROUP BY B  
UNION ALL  
SELECT 0 FROM T GROUP BY A,B
```

对于只对一个字段（或字段表达式）进行分组查询，`GROUPING (<column_expression>)` 和 `GROUPING_ID(<column_expression>)` 是等价对。

### Example

在开始我们的例子之前，我们先准备好以下数据：

```sql
CREATE TABLE employee (
  uid        INT,
  name       VARCHAR(32),
  level      VARCHAR(32),
  title      VARCHAR(32),
  department VARCHAR(32),
  hiredate   DATE
)
UNIQUE KEY(uid)
DISTRIBUTED BY HASH(uid) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
);

INSERT INTO employee VALUES
  (1, 'Abby', 'Senior', 'President', 'Board of Directors', '1999-11-13'),
  (2, 'Bob', 'Senior', 'Vice-President', 'Board of Directors', '1999-11-13'),
  (3, 'Candy', 'Senior', 'System Engineer', 'Technology', '2005-3-7'),
  (4, 'Devere', 'Senior', 'Hardware Engineer', 'Technology', '2006-7-9'),
  (5, 'Emilie', 'Senior', 'System Analyst', 'Technology', '2003-8-28'),
  (6, 'Fredrick', 'Senior', 'Sales Manager', 'Sales', '2004-9-7'),
  (7, 'Gitel', 'Assistant', 'Business Executive', 'Sales', '2003-3-19'),
  (8, 'Haden', 'Trainee', 'Sales Assistant', 'Sales', '2007-6-30'),
  (9, 'Irene', 'Assistant', 'Business Executive', 'Sales', '2005-10-20'),
  (10, 'Jankin', 'Senior', 'Marketing Supervisor', 'Marketing', '2001-4-13'),
  (11, 'Louis', 'Trainee', 'Marketing Assistant', 'Marketing', '2007-8-2'),
  (12, 'Martin', 'Trainee', 'Marketing Assistant', 'Marketing', '2007-7-1'),
  (13, 'Nasir', 'Assistant', 'Marketing Executive', 'Marketing', '2004-9-3');
```

结果如下：

```text
+------+----------+-----------+----------------------+--------------------+------------+
| uid  | name     | level     | title                | department         | hiredate   |
+------+----------+-----------+----------------------+--------------------+------------+
|    1 | Abby     | Senior    | President            | Board of Directors | 1999-11-13 |
|    2 | Bob      | Senior    | Vice-President       | Board of Directors | 1999-11-13 |
|    3 | Candy    | Senior    | System Engineer      | Technology         | 2005-03-07 |
|    4 | Devere   | Senior    | Hardware Engineer    | Technology         | 2006-07-09 |
|    5 | Emilie   | Senior    | System Analyst       | Technology         | 2003-08-28 |
|    6 | Fredrick | Senior    | Sales Manager        | Sales              | 2004-09-07 |
|    7 | Gitel    | Assistant | Business Executive   | Sales              | 2003-03-19 |
|    8 | Haden    | Trainee   | Sales Assistant      | Sales              | 2007-06-30 |
|    9 | Irene    | Assistant | Business Executive   | Sales              | 2005-10-20 |
|   10 | Jankin   | Senior    | Marketing Supervisor | Marketing          | 2001-04-13 |
|   11 | Louis    | Trainee   | Marketing Assistant  | Marketing          | 2007-08-02 |
|   12 | Martin   | Trainee   | Marketing Assistant  | Marketing          | 2007-07-01 |
|   13 | Nasir    | Assistant | Marketing Executive  | Marketing          | 2004-09-03 |
+------+----------+-----------+----------------------+--------------------+------------+
13 rows in set (0.01 sec)
```

#### A. Using GROUPING_ID to identify grouping levels

下面的例子按部门和职级统计雇员的人数。GROUPING_ID() 函数被用来计算每一行的聚合程度，其结果放在 `Job Title` 这一列上。

```sql
SELECT
  department,
  CASE 
  	WHEN GROUPING_ID(department, level) = 0 THEN level
  	WHEN GROUPING_ID(department, level) = 1 THEN CONCAT('Total: ', department)
  	WHEN GROUPING_ID(department, level) = 3 THEN 'Total: Company'
  	ELSE 'Unknown'
  END AS 'Job Title',
  COUNT(uid) AS 'Employee Count'
FROM employee 
GROUP BY ROLLUP(department, level)
ORDER BY GROUPING_ID(department, level) ASC;
```

结果如下：

```text
+--------------------+---------------------------+----------------+
| department         | Job Title                 | Employee Count |
+--------------------+---------------------------+----------------+
| Board of Directors | Senior                    |              2 |
| Technology         | Senior                    |              3 |
| Sales              | Senior                    |              1 |
| Sales              | Assistant                 |              2 |
| Sales              | Trainee                   |              1 |
| Marketing          | Senior                    |              1 |
| Marketing          | Trainee                   |              2 |
| Marketing          | Assistant                 |              1 |
| Board of Directors | Total: Board of Directors |              2 |
| Technology         | Total: Technology         |              3 |
| Sales              | Total: Sales              |              4 |
| Marketing          | Total: Marketing          |              4 |
| NULL               | Total: Company            |             13 |
+--------------------+---------------------------+----------------+
13 rows in set (0.01 sec)
```

#### B. Using GROUPING_ID to filter a result set

在下面的代码中，将返回部门中的高级人员的行。

```sql
SELECT
  department,
  CASE 
  	WHEN GROUPING_ID(department, level) = 0 THEN level
  	WHEN GROUPING_ID(department, level) = 1 THEN CONCAT('Total: ', department)
  	WHEN GROUPING_ID(department, level) = 3 THEN 'Total: Company'
  	ELSE 'Unknown'
  END AS 'Job Title',
  COUNT(uid)
FROM employee 
GROUP BY ROLLUP(department, level)
HAVING `Job Title` IN ('Senior');
```

结果如下：

```text
+--------------------+-----------+--------------+
| department         | Job Title | count(`uid`) |
+--------------------+-----------+--------------+
| Board of Directors | Senior    |            2 |
| Technology         | Senior    |            3 |
| Sales              | Senior    |            1 |
| Marketing          | Senior    |            1 |
+--------------------+-----------+--------------+
5 rows in set (0.01 sec)
```

### Keywords

GROUPING_ID

### Best Practice

更多信息可以参考：
- [GROUPING](./grouping.md)
