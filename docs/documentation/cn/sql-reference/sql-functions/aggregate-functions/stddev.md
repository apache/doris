# STDDEV

## Syntax

`STDDEV(expr)`

## Description

返回expr表达式的标准差

## Examples
```
MySQL > select stddev(scan_rows) from log_statis group by datetime;
+---------------------+
| stddev(`scan_rows`) |
+---------------------+
|  2.3736656687790934 |
+---------------------+
```
