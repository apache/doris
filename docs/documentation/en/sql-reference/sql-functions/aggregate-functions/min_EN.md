# MIN
## Description
### Syntax

`MIN(expr)`


Returns the minimum value of an expr expression

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
