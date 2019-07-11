# MIN

## Syntax

`MIN(expr)`

## Description

返回expr表达式的最小值

## Examples
```
MySQL > select min(scan_rows) from log_statis group by datetime;
+------------------+
| min(`scan_rows`) |
+------------------+
|                0 |
+------------------+
```
