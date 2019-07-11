# PERCENTILE_APPROX

## Syntax

`PERCENTILE_APPROX(expr, DOUBLE p)`

## Description

返回第p个百分位点的近似值，p的值介于0到1之间

该函数使用固定大小的内存，因此对于高基数的列可以使用更少的内存，可用于计算tp99等统计值

## Examples
```
MySQL > select `table`, percentile_approx(cost_time,0.99) from log_statis group by `table`;
+---------------------+---------------------------+
| table    | percentile_approx(`cost_time`, 0.99) |
+----------+--------------------------------------+
| test     |                                54.22 |
+----------+--------------------------------------+
```