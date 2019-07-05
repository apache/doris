# VARIANCE_SAMP

## Syntax

`VARIANCE_SAMP(expr)`

## Description

返回expr表达式的无偏样本方差

## Examples
```
MySQL > select variance_samp(scan_rows) from log_statis group by datetime;
+----------------------------+
| variance_samp(`scan_rows`) |
+----------------------------+
|          5.578615881581948 |
+----------------------------+
```