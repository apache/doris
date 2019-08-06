# STDDEV,STDDEV_POP
## description

返回expr表达式的标准差

 Syntax

`STDDEV(expr)`

## example
```
MySQL > select stddev(scan_rows) from log_statis group by datetime;
+---------------------+
| stddev(`scan_rows`) |
+---------------------+
|  2.3736656687790934 |
+---------------------+

MySQL > select stddev_pop(scan_rows) from log_statis group by datetime;
+-------------------------+
| stddev_pop(`scan_rows`) |
+-------------------------+
|      2.3722760595994914 |
+-------------------------+
```
##keyword
STDDEV,STDDEV_POP,STDDEV,STDDEV,POP
