---
{
    "title": "SQL Dialect",
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

# SQL Dialect

Starting from version 2.1, Doris can support multiple SQL dialects, such as Presto, Trino, Hive, PostgreSQL, Spark, Oracle, Clickhouse, and more. Through this feature, users can directly use the corresponding SQL dialect to query data in Doris, which facilitates users to smoothly migrate their original business to Doris.

> 1. This function is currently an experimental function. If you encounter any problems during use, you are welcome to provide feedback through the mail group, [GitHub issue](https://github.com/apache/doris/issues), etc. .
>
> 2. This function only supports query statements and does not support other DDL and DML statements including Explain.

## Deploy service

1. Download latest [Doris SQL Convertor(1.0.1)](https://doris-build-1308700295.cos.ap-beijing.myqcloud.com/transform-doris-tool/doris-sql-convertor-1.0.1-bin-x86)
2. On any FE node, start the service through the following command:

	`nohup ./doris-sql-convertor-1.0.1-bin-x86 run --host=0.0.0.0 --port=5001 &`
	
    :::note
	1. This service is a stateless service and can be started and stopped at any time.
	
	2. `5001` is the service port and can be arbitrarily specified as an available port.
	
	3. It is recommended to start a separate service on each FE node.
    :::

3. Start the Doris cluster (version 2.1 or higher)
4. Set the URL of the SQL Dialect Conversion Service with the following command in Doris:

	`MySQL> set global sql_converter_service_url = "http://127.0.0.1:5001/api/v1/convert"`

	:::note
	1. `127.0.0.1:5001` is the deployment node IP and port of the SQL dialect conversion service.
    :::
	
## Use SQL dialect

Currently supported dialect types include:

- `presto`
- `trino`
- `hive`
- `spark`
- `postgres`
- `clickhouse`
- `oracle`

example:

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

