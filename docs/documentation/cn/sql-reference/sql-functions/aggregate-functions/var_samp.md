# VAR_SAMP

## Syntax

`VAR_SAMP(expr)`

## Description

返回expr表达式的无偏样本方差

## Examples
```
MySQL > select var_samp(scan_rows) from log_statis group by datetime;
+-----------------------+
| var_samp(`scan_rows`) |
+-----------------------+
|    5.6227132145741789 |
+-----------------------+
```