---
{
    "title": "GROUPING_ID",
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

## GROUPING_ID

### Name

GROUPING_ID

### Description

Is a function that computes the level of grouping. `GROUPING_ID` can be used only in the `SELECT <select> list`, `HAVING`, or `ORDER BY` clauses when `GROUP BY` is specified.

#### Syntax

```sql
GROUPING_ID ( <column_expression>[ ,...n ] )
```

#### Arguments

`<column_expression>`

Is a `column_expression` in a GROUP BY clause.

#### Return Type

BIGINT

#### Remarks

The GROUPING_ID's `<column_expression>` must exactly match the expression in the `GROUP BY` list. For example, if you are grouping by `user_id`, use `GROUPING_ID (user_id)`; or if you are grouping by `name`, use `GROUPING_ID (name)`.

#### Comparing GROUPING_ID() to GROUPING()

`GROUPING_ID(<column_expression> [ ,...n ])` inputs the equivalent of the `GROUPING(<column_expression>)` return for each column in its column list in each output row as a string of ones and zeros. GROUPING_ID interprets that string as a base-2 number and returns the equivalent integer. For example consider the following statement: `SELECT a, b, c, SUM(d), GROUPING_ID(a,b,c) FROM T GROUP BY <group by list>`. The following table shows the GROUPING_ID() input and output values.

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

Each GROUPING_ID argument must be an element of the GROUP BY list. GROUPING_ID() returns an integer bitmap whose lowest N bits may be lit. A lit bit indicates the corresponding argument is not a grouping column for the given output row. The lowest-order bit corresponds to argument N, and the (N-1)ᵗʰ lowest-order bit corresponds to argument 1.

#### GROUPING_ID() Equivalents

For a single grouping query, `GROUPING (<column_expression>)` is equivalent to `GROUPING_ID(<column_expression>)`, and both return 0.
For example, the following statements are equivalent:

Statement A:

```sql
SELECT GROUPING_ID(A,B)  
FROM T   
GROUP BY CUBE(A,B)
```

Statement B:

```sql
SELECT 3 FROM T GROUP BY ()  
UNION ALL  
SELECT 1 FROM T GROUP BY A  
UNION ALL  
SELECT 2 FROM T GROUP BY B  
UNION ALL  
SELECT 0 FROM T GROUP BY A,B
```

### Example

Before starting our example, We first prepare the following data.

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

Here is the result.

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

The following example returns the count of employees by `department` and `level`. GROUPING_ID() is used to create a value for each row in the `Job Title` column that identifies its level of aggregation.

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

Here is the result.

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

In the following code, to return only the rows that have the count of  senior in department.

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

Here is the result.

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

For more information, see also:
- [GROUPING](./grouping.md)
