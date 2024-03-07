---
{
    "title": "SQL方言兼容",
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

# SQL方言兼容

从 2.1 版本开始，Doris 可以支持多种 SQL 方言，如 Presto、Trino、Hive、PostgreSQL、Spark、Clickhouse 等等。通过这个功能，用户可以直接使用对应的 SQL 方言查询 Doris 中的数据，方便用户将原先的业务平滑的迁移到 Doris 中。

> 1. 该功能目前是实验性功能，您在使用过程中如遇到任何问题，欢迎通过邮件组、[GitHub issue](https://github.com/apache/doris/issues) 等方式进行反馈。
> 
> 2. 该功能只支持查询语句，不支持 DDL、DML 语句。

## 部署服务

1. 下载最新版本的 [SQL 方言转换工具](https://doris-build-1308700295.cos.ap-beijing.myqcloud.com/transform-doris-tool/transform-doris-tool-1.0.0-bin-x86)。
2.  在任意 FE 节点，通过以下命令启动服务：

	`nohup ./doris-sql-convertor-1.0.1-bin-x86 run --host=0.0.0.0 --port=5001 &`

    :::note
	 1. 该服务是一个无状态的服务，可随时启停。
	
	 2. `5001` 是服务端口，可以任意指定为一个可用端口。
	
	 3. 建议在每个 FE 节点都单独启动一个服务。
    :::

3. 启动 Doris 集群（2.1 或更高版本）
4. 通过以下命令，在 Doris 中设置 SQL 方言转换服务的 URL：

	`MySQL> set global sql_converter_service_url = "http://127.0.0.1:5001/api/v1/convert"`

	:::note
	1. `127.0.0.1:5001` 是 SQL 方言转换服务的部署节点 ip 和端口。
    :::
	
## 使用 SQL 方言

目前支持的方言类型包括：

- `presto`
- `trino`
- `hive`
- `spark`
- `postgres`
- `clickhouse`

示例:

- Presto

```sql
mysql> set sql_dialect=presto;
Query OK, 0 rows affected (0.00 sec)

mysql> SELECT cast(start_time as varchar(20)) as col1,
    ->        array_distinct(arr_int) as col2,
    ->        FILTER(arr_str, x -> x LIKE '%World%') as col3,
    ->        to_date(value,'%Y-%m-%d')as col4,
    ->        YEAR(start_time) as col5,
    ->        date_add('month', 1, start_time) as col6,
    ->        date_format(start_time, '%Y%m%d')as col7,
    ->        REGEXP_EXTRACT_ALL(value, '-.') as col8,
    ->        JSON_EXTRACT('{"id": "33"}', '$.id')as col9,
    ->        element_at(arr_int, 1) as col10,
    ->        date_trunc('day',start_time) as col11
    ->     FROM test_sqlconvert
    ->     where date_trunc('day',start_time)= DATE'2024-05-20'     
    -> order by id; 
+---------------------+-----------+-----------+-------------+------+---------------------+----------+-------------+------+-------+---------------------+
| col1                | col2      | col3      | col4        | col5 | col6                | col7     | col8        | col9 | col10 | col11               |
+---------------------+-----------+-----------+-------------+------+---------------------+----------+-------------+------+-------+---------------------+
| 2024-05-20 13:14:52 | [1, 2, 3] | ["World"] | 2024-01-14  | 2024 | 2024-06-20 13:14:52 | 20240520 | ['-0','-1'] | "33" |     1 | 2024-05-20 00:00:00 |
+---------------------+-----------+-----------+-------------+------+---------------------+----------+-------------+------+-------+---------------------+
1 row in set (0.13 sec)

```

- Clickhouse

```sql
mysql> set sql_dialect=clickhouse;
Query OK, 0 rows affected (0.01 sec)

mysql> select toString(start_time) as col1,
    ->        arrayCompact(arr_int) as col2,
    ->        arrayFilter(x -> x like '%World%',arr_str)as col3,
    ->        toDate(value) as col4,
    ->        toYear(start_time)as col5,
    ->        addMonths(start_time, 1)as col6,
    ->        toYYYYMMDD(start_time, 'US/Eastern')as col7,
    ->        extractAll(value, '-.')as co8,
    ->        JSONExtractString('{"id": "33"}' , 'id')as col9,
    ->        arrayElement(arr_int, 1) as col10,
    ->        date_trunc('day',start_time) as col11
    ->      FROM test_sqlconvert
    ->      where date_trunc('day',start_time)= '2024-05-20 00:00:00'
    -> order by id;
+---------------------+-----------+-----------+------------+------+---------------------+----------+-------------+------+-------+---------------------+
| col1                | col2      | col3      | col4       | col5 | col6                | col7     | co8         | col9 | col10 | col11               |
+---------------------+-----------+-----------+------------+------+---------------------+----------+-------------+------+-------+---------------------+
| 2024-05-20 13:14:52 | [1, 2, 3] | ["World"] | 2024-01-14 | 2024 | 2024-06-20 13:14:52 | 20240520 | ['-0','-1'] | "33" |     1 | 2024-05-20 00:00:00 |
+---------------------+-----------+-----------+------------+------+---------------------+----------+-------------+------+-------+---------------------+
1 row in set (0.04 sec)
```

