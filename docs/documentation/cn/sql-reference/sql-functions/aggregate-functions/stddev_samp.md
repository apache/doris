# STDDEV_SAMP
## description

返回expr表达式的样本标准差

 Syntax

`STDDEV_SAMP(expr)`

## example
```
MySQL > select stddev_samp(scan_rows) from log_statis group by datetime;
+--------------------------+
| stddev_samp(`scan_rows`) |
+--------------------------+
|        2.372044195280762 |
+--------------------------+
```
##keyword
STDDEV_SAMP,STDDEV,SAMP
