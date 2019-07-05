# COUNT_DISTINCT

## Syntax

`COUNT_DISTINCT(expr)`

## Description

用于返回满足要求的行的数目，或者非NULL行的数目

## Examples

```
MySQL > select count_distinct(query_id) from log_statis group by datetime;
+----------------------------+
| count_distinct(`query_id`) |
+----------------------------+
|                        577 |
+----------------------------+
```
