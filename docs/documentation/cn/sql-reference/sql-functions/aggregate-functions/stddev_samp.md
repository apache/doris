# STDDEV_SAMP

## Syntax

`STDDEV_SAMP(expr)`

## Description

返回expr表达式的样本标准差

## Examples
```
MySQL > select stddev_samp(scan_rows) from log_statis group by datetime;
+--------------------------+
| stddev_samp(`scan_rows`) |
+--------------------------+
|        2.372044195280762 |
+--------------------------+
```
