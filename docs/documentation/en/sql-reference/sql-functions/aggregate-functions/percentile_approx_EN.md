# PERCENTILE_APPROX
## Description
### Syntax

`PERCENTILE_APPROX(expr, DOUBLE p[, DOUBLE compression])`

Return the approximation of the point p, where the value of P is between 0 and 1.

Compression param is optional and can be setted to a value in the range of (0, 10000). The bigger compression you set, the more precise result and more time cost you will get. If it is not setted or not setted in the correct range, PERCENTILE_APPROX function will run with a default compression param of 10000.

This function uses fixed size memory, so less memory can be used for columns with high cardinality, and can be used to calculate statistics such as tp99.

## example
```
MySQL > select `table`, percentile_approx(cost_time,0.99) from log_statis group by `table`;
+---------------------+---------------------------+
| table    | percentile_approx(`cost_time`, 0.99) |
+----------+--------------------------------------+
| test     |                                54.22 |
+----------+--------------------------------------+
MySQL > select `table`, percentile_approx(cost_time,0.99, 100) from log_statis group by `table`;
+---------------------+---------------------------+
| table    | percentile_approx(`cost_time`, 0.99, 100) |
+----------+--------------------------------------+
| test     |                                54.21 |
+----------+--------------------------------------+
##keyword
PERCENTILE_APPROX,PERCENTILE,APPROX
