---
{
    "title": "CREATE MATERIALIZED VIEW",
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

# CREATE MATERIALIZED VIEW

## description

    该语句用于创建物化视图。

说明: 
    异步语法，调用成功后仅表示创建物化视图的任务提交成功，用户需要先通过 ``` show alter table rollup ``` 来查看物化视图的创建进度。
    在显示 FINISHED 后既可通过 ``` desc [table_name] all ``` 命令来查看物化视图的 schema 了。
    
语法:

    ```

    CREATE MATERIALIZED VIEW [MV name] as [query]
    [PROPERTIES ("key" = "value")]

    ```

1. MV name
	
	物化视图的名称，必填项。
	
	相同表的物化视图名称不可重复。
	
2. query

	用于构建物化视图的查询语句，查询语句的结果既物化视图的数据。目前支持的 query 格式为:
	
	```
	
    SELECT select_expr[, select_expr ...]
    FROM [Base view name]
    GROUP BY column_name[, column_name ...]
    ORDER BY column_name[, column_name ...]
    
    语法和查询语句语法一致。
    
	```
	
	select_expr: 物化视图的 schema 中所有的列。
		+ 仅支持不带表达式计算的单列，聚合列。
		+ 其中聚合函数目前仅支持 SUM, MIN, MAX 三种，且聚合函数的参数只能是不带表达式计算的单列。
		+ 至少包含一个单列。
		+ 所有涉及到的列，均只能出现一次。
	
	base view name: 物化视图的原始表名，必填项。
		+ 必须是单表，且非子查询
	
	group by: 物化视图的分组列，选填项。
		+ 不填则数据不进行分组。
	
	order by: 物化视图的排序列，选填项。
		+ 排序列的声明顺序必须和 select_expr 中列声明顺序一致。
		+ 如果不声明 order by，则根据规则自动补充排序列。
		      如果物化视图是聚合类型，则所有的分组列自动补充为排序列。
		      如果物化视图是非聚合类型，则前 36 个字节自动补充为排序列。如果自动补充的排序个数小于3个，则前三个作为排序列。
		+ 如果 query 中包含分组列的话，则排序列必须和分组列一致。

3. properties

	声明物化视图的一些配置，选填项。
	
	```
	
	PROPERTIES ("key" = "value", "key" = "value" ...)
	
	```
	
	以下几个配置，均可声明在此处：
	
		short_key: 排序列的个数。
		timeout: 物化视图构建的超时时间。
		
## example

Base 表结构为

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

1. 创建一个仅包含原始表 （k1, k2）列的物化视图

	```
	create materialized view k1_k2 as
	    select k1, k2 from duplicate_table;
	```
	
	物化视图的 schema 如下图，物化视图仅包含两列 k1, k2 且不带任何聚合
		
	```
	+-----------------+-------+--------+------+------+---------+-------+
	| IndexName       | Field | Type   | Null | Key  | Default | Extra |
	+-----------------+-------+--------+------+------+---------+-------+
	| k1_k2           | k1    | INT    | Yes  | true | N/A     |       |
	|                 | k2    | INT    | Yes  | true | N/A     |       |
	+-----------------+-------+--------+------+------+---------+-------+
	```
	
2. 创建一个以 k2 为排序列的物化视图
	
	```
	create materialized view k2_order as
	    select k2, k1 from duplicate_table order by k2;
	```
	
	物化视图的 schema 如下图，物化视图仅包含两列 k2, k1，其中 k2 列为排序列，不带任何聚合。
	
	```
	+-----------------+-------+--------+------+-------+---------+-------+
	| IndexName       | Field | Type   | Null | Key   | Default | Extra |
	+-----------------+-------+--------+------+-------+---------+-------+
	| k2_order        | k2    | INT    | Yes  | true  | N/A     |       |
	|                 | k1    | INT    | Yes  | false | N/A     | NONE  |
	+-----------------+-------+--------+------+-------+---------+-------+
	```
	
3. 创建一个以 k1, k2 分组，k3 列为 SUM 聚合的物化视图

	```
	create materialized view k1_k2_sumk3 as
	    select k1, k2, sum(k3) from duplicate_table group by k1, k2;
	```
	
	物化视图的 schema 如下图，物化视图包含两列 k1, k2，sum(k3) 其中 k1, k2 为分组列，sum(k3) 为根据 k1, k2 分组后的 k3 列的求和值。
	
	由于物化视图没有声明排序列，且物化视图带聚合数据，系统默认补充分组列 k1, k2 为排序列。
	
	```
	+-----------------+-------+--------+------+-------+---------+-------+
	| IndexName       | Field | Type   | Null | Key   | Default | Extra |
	+-----------------+-------+--------+------+-------+---------+-------+
	| k1_k2_sumk3     | k1    | INT    | Yes  | true  | N/A     |       |
	|                 | k2    | INT    | Yes  | true  | N/A     |       |
	|                 | k3    | BIGINT | Yes  | false | N/A     | SUM   |
	+-----------------+-------+--------+------+-------+---------+-------+
	```
	
4. 创建一个去除重复行的物化视图

	```
	create materialized view deduplicate as
	    select k1, k2, k3, k4 from duplicate_table group by k1, k2, k3, k4;
	```
	
	物化视图 schema 如下图，物化视图包含 k1, k2, k3, k4列，且不存在重复行。
	
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
	
5. 创建一个不声明排序列的非聚合型物化视图

	all_type_table 的 schema 如下：
	
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
	
	物化视图包含 k3, k4, k5, k6, k7 列，且不声明排序列，则创建语句如下：

	```
	create materialized view mv_1 as
	    select k3, k4, k5, k6, k7 from all_type_table;
	```
	
	系统默认补充的排序列为 k3, k4, k5 三列。这三列类型的字节数之和为 4(INT) + 8(BIGINT) + 16(DECIMAL) = 28 < 36。所以补充的是这三列作为排序列。
	物化视图的 schema 如下，可以看到其中 k3, k4, k5 列的 key 字段为 true，也就是排序列。k6, k7 列的 key 字段为 false，也就是非排序列。
	
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
