# GROUP_CONCAT	

## Syntax

`GROUP_CONCAT(expr)`

## Description

用于返回选中字段字符串连接起来的新字符串

使用逗号连接

## Examples
```
MySQL> select group_concat(`query_id`) from log_statis group by datetime;
+-----------------------------------+
| group_concat(`query_id`)          |
+-----------------------------------+
| 445f5875f8854dfa:b9358d5cd86488a2 |
+-----------------------------------+
```
