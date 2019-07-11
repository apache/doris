# MAX

## Syntax

`MAX(expr)`

## Description

返回expr表达式的最大值

## Examples
```
MySQL > select max(scan_rows) from log_statis group by datetime;
+------------------+
| max(`scan_rows`) |
+------------------+
|          4671587 |
+------------------+
```
