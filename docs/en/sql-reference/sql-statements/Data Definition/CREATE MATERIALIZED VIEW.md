---
{
    "title": "CREATE MATERIALIZED VIEW",
    "language": "en"
}
---

<!-
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
->

# CREATE MATERIALIZED VIEW

## description

This statement is used to create a materialized view.

    Asynchronous syntax. After the call is successful, it only indicates that the task to create the materialized view is successfully submitted. The user needs to check the progress of the materialized view by using ```show alter table rollup```.
    
    After the progress is FINISHED, you can use the ```desc [table_name] all``` command to view the schema of the materialized view.

syntax:

    ```

    CREATE MATERIALIZED VIEW [MG name] as [query]
    [PROPERTIES ("key" = "value")]

    ```

1. MV name

	Name of the materialized view. Required.

	Materialized view names in the same table cannot be duplicated.

2. query

	The query used to construct the materialized view. The result of the query is the data of the materialized view. The query format currently supported is:

	```
    SELECT select_expr [, select_expr ...]
    FROM [Base view name]
    GROUP BY column_name [, column_name ...]
    ORDER BY column_name [, column_name ...]
    
    The syntax is the same as the query syntax.
	```

	select_expr: All columns in the materialized view's schema.
	
	+ Only single columns and aggregate columns without expression calculation are supported.
	+ The aggregate function currently only supports SUM, MIN, MAX, and the parameters of the aggregate function can only be a single column without expression calculation.
	+ Contains at least one single column.
	+ All involved columns can only appear once.

	base view name: The original table name of the materialized view. Required.
	
	+ Must be a single table and not a subquery

	group by: Grouped column of materialized view, optional.
	
	+ If not filled, the data will not be grouped.

	order by: Sort order of materialized view, optional.
	
	+ The order of the column sort must be the same as the column declaration order in select_expr.
	+ If order by is not specified, sort columns are automatically supplemented according to the rules.
		
		+ If the materialized view is an aggregate type, all grouping columns are automatically supplemented with sort columns.
		+ If the materialized view is a non-aggregated type, the first 36 bytes are automatically supplemented as a sorted column. If the number of sorts for automatic replenishment is less than three, the first three are sorted.
	+ If the query contains a grouping column, the sort order must be the same as the grouping column.

3. properties

	Declare some configuration of materialized view, optional.

	```
	PROPERTIES ("key" = "value", "key" = "value" ...)

	```

	The following configurations can be declared here:
	
	+ short_key: the number of columns.
	+ timeout: timeout for materialized view construction.

## example

Base table structure is

```
mysql> desc duplicate_table;
+-------+--------+------+------+---------+-------+
| Field | Type   | Null | Key  | Default | Extra |
+-------+--------+------+------+---------+-------+
| k1    | INT    | Yes  | true | N/A     |       |
| k2    | INT    | Yes  | true | N/A     |       |
| k3    | BIGINT | Yes  | true | N/A     |       |
| k4    | BIGINT | Yes  | true | N/A     |       |
+-------+--------+------+------+---------+-------+
```

1. Create a materialized view containing only the columns of the original table (k1, k2)

	```
	create materialized view k1_k2 as
select k1, k2 from duplicate_table;
	```

	The materialized view's schema is shown below. The materialized view contains only two columns k1, k2 without any aggregation.

	```
	+-----------------+-------+--------+------+------+---------+-------+
	| IndexName       | Field | Type   | Null | Key  | Default | Extra |
	+-----------------+-------+--------+------+------+---------+-------+
	| k1_k2           | k1    | INT    | Yes  | true | N/A     |       |
	|                 | k2    | INT    | Yes  | true | N/A     |       |
	+-----------------+-------+--------+------+------+---------+-------+
	```

2. Create a materialized view sorted by k2

	```
	create materialized view k2_order as
select k2, k1 from duplicate_table order by k2;
```

	The materialized view's schema is shown below. The materialized view contains only two columns k2, k1, where column k2 is a sorted column without any aggregation.

	```
	+-----------------+-------+--------+------+-------+---------+-------+
	| IndexName       | Field | Type   | Null | Key   | Default | Extra |
	+-----------------+-------+--------+------+-------+---------+-------+
	| k2_order        | k2    | INT    | Yes  | true  | N/A     |       |
	|                 | k1    | INT    | Yes  | false | N/A     | NONE  |
	+-----------------+-------+--------+------+-------+---------+-------+
	```

3. Create a materialized view grouped by k1, k2 with k3 as the SUM aggregate

	```
	create materialized view k1_k2_sumk3 as
select k1, k2, sum (k3) from duplicate_table group by k1, k2;
	```

	The materialized view's schema is shown below. The materialized view contains two columns k1, k2 and sum (k3), where k1, k2 are grouped columns, and sum (k3) is the sum of the k3 columns grouped according to k1, k2.

	Because the materialized view does not declare a sort column, and the materialized view has aggregate data, the system supplements the grouping columns k1 and k2 by default.

	```
	+-----------------+-------+--------+------+-------+---------+-------+
	| IndexName       | Field | Type   | Null | Key   | Default | Extra |
	+-----------------+-------+--------+------+-------+---------+-------+
	| k1_k2_sumk3     | k1    | INT    | Yes  | true  | N/A     |       |
	|                 | k2    | INT    | Yes  | true  | N/A     |       |
	|                 | k3    | BIGINT | Yes  | false | N/A     | SUM   |
	+-----------------+-------+--------+------+-------+---------+-------+
	```

4. Create a materialized view to remove duplicate rows

	```
	create materialized view deduplicate as
select k1, k2, k3, k4 from duplicate_table group by k1, k2, k3, k4;
	```

	The materialized view schema is shown below. The materialized view contains k1, k2, k3, and k4 columns, and there are no duplicate rows.

	```
	+-----------------+-------+--------+------+-------+---------+-------+
	| IndexName       | Field | Type   | Null | Key   | Default | Extra |
	+-----------------+-------+--------+------+-------+---------+-------+
	| deduplicate     | k1    | INT    | Yes  | true  | N/A     |       |
	|                 | k2    | INT    | Yes  | true  | N/A     |       |
	|                 | k3    | BIGINT | Yes  | true  | N/A     |       |
	|                 | k4    | BIGINT | Yes  | true  | N/A     |       |
	+-----------------+-------+--------+------+-------+---------+-------+
	```

5. Create a non-aggregated materialized view that does not declare a sort column

	The schema of all_type_table is as follows:

	```
	+-------+--------------+------+-------+---------+-------+
	| Field | Type         | Null | Key   | Default | Extra |
	+-------+--------------+------+-------+---------+-------+
	| k1    | TINYINT      | Yes  | true  | N/A     |       |
	| k2    | SMALLINT     | Yes  | true  | N/A     |       |
	| k3    | INT          | Yes  | true  | N/A     |       |
	| k4    | BIGINT       | Yes  | true  | N/A     |       |
	| k5    | DECIMAL(9,0) | Yes  | true  | N/A     |       |
	| k6    | DOUBLE       | Yes  | false | N/A     | NONE  |
	| k7    | VARCHAR(20)  | Yes  | false | N/A     | NONE  |
	+-------+--------------+------+-------+---------+-------+
	```

	The materialized view contains k3, k4, k5, k6, k7 columns, and no sort column is declared. The creation statement is as follows:

	```
	create materialized view mv_1 as
select k3, k4, k5, k6, k7 from all_type_table;
	```

	The system's default supplementary sort columns are k3, k4, and k5. The sum of the number of bytes for these three column types is 4 (INT) + 8 (BIGINT) + 16 (DECIMAL) = 28 <36. So these three columns are added as sort columns.
	
	The materialized view's schema is as follows. You can see that the key fields of the k3, k4, and k5 columns are true, which is the sort order. The key field of the k6, k7 columns is false, that is, non-sorted.

	```
	+----------------+-------+--------------+------+-------+---------+-------+
	| IndexName      | Field | Type         | Null | Key   | Default | Extra |
	+----------------+-------+--------------+------+-------+---------+-------+
	| mv_1           | k3    | INT          | Yes  | true  | N/A     |       |
	|                | k4    | BIGINT       | Yes  | true  | N/A     |       |
	|                | k5    | DECIMAL(9,0) | Yes  | true  | N/A     |       |
	|                | k6    | DOUBLE       | Yes  | false | N/A     | NONE  |
	|                | k7    | VARCHAR(20)  | Yes  | false | N/A     | NONE  |
	+----------------+-------+--------------+------+-------+---------+-------+
	```
	
## keyword
  CREATE, MATERIALIZED, VIEW
