# MIN
## description
### Syntax

`MIN(expr)`


返回expr表达式的最小值

## example
```
MySQL > select min(scan_rows) from log_statis group by datetime;
+------------------+
| min(`scan_rows`) |
+------------------+
|                0 |
+------------------+
```
##keyword
MIN
