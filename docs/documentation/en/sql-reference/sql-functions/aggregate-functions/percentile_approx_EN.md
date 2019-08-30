# PERCENTILE_APPROX
## Description
### Syntax

`PERCENTILE_APPROX(expr, DOUBLE p)`


Return the approximation of the point p, where the value of P is between 0 and 1.

This function uses fixed size memory, so less memory can be used for columns with high cardinality, and can be used to calculate statistics such as tp99.

## example
```
MySQL > select `table`, percentile_approx(cost_time,0.99) from log_statis group by `table`;
+---------------------+---------------------------+
| table    | percentile_approx(`cost_time`, 0.99) |
+----------+--------------------------------------+
| test     |                                54.22 |
+----------+--------------------------------------+
##keyword
PERCENTILE_APPROX,PERCENTILE,APPROX
