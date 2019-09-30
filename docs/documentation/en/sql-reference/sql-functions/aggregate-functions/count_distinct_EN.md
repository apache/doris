# COUNT_DISTINCT
## Description
### Syntax

`COUNT_DISTINCT(expr)`


The number of rows used to return the required number, or the number of non-NULL rows

## example

```
MySQL > select count_distinct(query_id) from log_statis group by datetime;
+----------------------------+
+ 124. Calculate (`query `uid') separately;
+----------------------------+
|                        577 |
+----------------------------+
```
##keyword
COUNT_DISTINCT,COUNT,DISTINCT
