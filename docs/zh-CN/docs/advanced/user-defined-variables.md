---
{
    "title": "用户自定义变量",
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


自定义变量指的是用户可以根据自身需求通过SQL语句将值存储在自定义的变量中，然后该变量可以被其他SQL语句引用。通过这种方式既可以实现值的传递，又可以简化SQL的编写。

## 使用指南
用户自定义的变量形式为：@var_name，其中变量名称由字母、数字、“.”、“_”、“¥”、“$”组成。不过，在以字符串或者标识符引用时也可以包含其他字符（例如：@\`var-name`），不支持纯数字和单独“.”的变量。

## 语法
用户自定义变量可以通过set语句定义

```sql
SET @var_name = expr [, @var_name = expr ...];
```

也可以使用:= 用作赋值运算符

```sql
SET @var_name := expr [, @var_name = expr ...];
```

- 声明自定义变量时，必须加前缀 @;
- 可同时声明多个自定义变量，多个变量之间用逗号 (,) 隔开;
- 支持多次声明同一自定义变量，新声明的值会覆盖原有值;
- expr暂不支持表达式;
- 如在一个 SQL 语句中引用了没有声明过的变量，该变量值默认为 NULL 且为 STRING 类型；

读取用户自定义变量可以通过select语句查询

```sql
SELECT @var_name [, @var_name ...];
```

## 使用限制
- 暂不支持查看用户已有的自定义变量；
- 暂不支持通过变量给变量赋值；
- 暂不支持声明 BITMAP、HLL、PERCENTILE 和 ARRAY 类型的自定义变量，JSON 类型的自定义变量会转换为 STRING 类型进行存储；
- 用户自定义变量属于会话级别的变量，当客户端断开时，其所有的会话变量会被释放；

## 示例

```sql
mysql> SET @v1=1, @v2:=2;
Query OK, 0 rows affected (0.00 sec)

mysql> select @v1,@v2;
+------+------+
| @v1  | @v2  |
+------+------+
|    1 |    2 |
+------+------+
1 row in set (0.00 sec)

mysql> select @v1+@v2;
+-------------+
| (@v1 + @v2) |
+-------------+
|           3 |
+-------------+
1 row in set (0.01 sec)

mysql> set @`var-name`=2;
Query OK, 0 rows affected (0.00 sec)

mysql> select @`var-name`;
+-----------+
| @var-name |
+-----------+
|         2 |
+-----------+
1 row in set (0.00 sec)

mysql> SET @j := '{"a": 1, "b": 2, "c": {"d": 4}}';
Query OK, 0 rows affected (0.00 sec)

mysql> select @j;
+---------------------------------+
| @j                              |
+---------------------------------+
| {"a": 1, "b": 2, "c": {"d": 4}} |
+---------------------------------+
1 row in set (0.00 sec)
```
 