# NDV

## Syntax

`NDV(expr)`

## Description

返回类似于 COUNT(DISTINCT col) 结果的近似值聚合函数。

它比 COUNT 和 DISTINCT 组合的速度更快，并使用固定大小的内存，因此对于高基数的列可以使用更少的内存。

## Examples
```
MySQL > select ndv(query_id) from log_statis group by datetime;
+-----------------+
| ndv(`query_id`) |
+-----------------+
| 17721           |
+-----------------+
```
