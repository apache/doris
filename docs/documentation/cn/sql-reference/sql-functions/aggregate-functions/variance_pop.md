# VARIANCE_POP

## Syntax

`VARIANCE_POP(expr)`

## Description

返回expr表达式的方差

## Examples
```
MySQL > select variance_pop(scan_rows) from log_statis group by datetime;
+---------------------------+
| variance_pop(`scan_rows`) |
+---------------------------+
|        5.5768547136359761 |
+---------------------------+
```