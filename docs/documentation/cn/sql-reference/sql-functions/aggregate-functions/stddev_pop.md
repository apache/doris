# STDDEV_POP

## Syntax

`STDDEV_POP(expr)`

## Description

返回expr表达式的标准差

## Examples
```
MySQL > select stddev_pop(scan_rows) from log_statis group by datetime;
+-------------------------+
| stddev_pop(`scan_rows`) |
+-------------------------+
|      2.3722760595994914 |
+-------------------------+
```
