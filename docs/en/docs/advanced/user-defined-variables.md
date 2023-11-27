---
{
    "title": "User Defined Variables",
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


User-defined variables refer to values that users can store in custom variables using SQL statements, and these variables can be referenced by other SQL statements. This approach facilitates value passing and simplifies SQL writing.

## Usage

User-defined variable takes the form: @var_name, where the variable name consists of letters, numbers, ".", "_", "¥", and "$". However, when referenced as a string or identifier, it can also include other characters (e.g. @\`var-name\`), excluding pure numbers and the standalone ".".

## Grammer

User-defined variables can be defined using the SET statement:

```sql
SET @var_name = expr [, @var_name = expr ...];
```

Or, using `:=` as the assignment operator:

```sql
SET @var_name := expr [, @var_name = expr ...];
```

- When declaring a user-defined variable, the @ prefix is mandatory.
- Multiple user-defined variables can be declared simultaneously, separated by commas (,).
- Multiple declarations of the same variable are allowed, and the newly declared value will override the original value.
- The `expr` currently does not support expressions.
- If an undeclared variable is referenced in a SQL statement, its value defaults to NULL, and its type is STRING.

User-defined variables can be queried using the SELECT statement:

```sql
SELECT @var_name [, @var_name ...];
```

## Use restrictions

- Viewing existing user-defined variables is not currently supported.
- Assigning a variable to another variable is not currently supported.
- Declaring BITMAP, HLL, PERCENTILE, and ARRAY type variables is not currently supported. JSON type variables are converted to STRING for storage.
- User-defined variables are session-level variables, and all session variables are released when the client disconnects.

## Example

```sql
mysql> SET @v1=1, @v2:=2;
Query OK, 0 rows affected (0.00 sec)

mysql> SELECT @v1,@v2;
+------+------+
| @v1  | @v2  |
+------+------+
|    1 |    2 |
+------+------+
1 row in set (0.00 sec)

mysql> SELECT @v1+@v2;
+-------------+
| (@v1 + @v2) |
+-------------+
|           3 |
+-------------+
1 row in set (0.01 sec)

mysql> SET @`var-name`=2;
Query OK, 0 rows affected (0.00 sec)

mysql> SELECT @`var-name`;
+-----------+
| @var-name |
+-----------+
|         2 |
+-----------+
1 row in set (0.00 sec)

mysql> SET @j := '{"a": 1, "b": 2, "c": {"d": 4}}';
Query OK, 0 rows affected (0.00 sec)

mysql> SELECT @j;
+---------------------------------+
| @j                              |
+---------------------------------+
| {"a": 1, "b": 2, "c": {"d": 4}} |
+---------------------------------+
1 row in set (0.00 sec)

```




