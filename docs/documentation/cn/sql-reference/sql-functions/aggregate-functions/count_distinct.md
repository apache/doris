# COUNT_DISTINCT
## description
### Syntax

`COUNT_DISTINCT(expr)`


用于返回满足要求的行的数目，或者非NULL行的数目

## example

```
MySQL > select count_distinct(query_id) from log_statis group by datetime;
+----------------------------+
| count_distinct(`query_id`) |
+----------------------------+
|                        577 |
+----------------------------+
```
##keyword
COUNT_DISTINCT,COUNT,DISTINCT
