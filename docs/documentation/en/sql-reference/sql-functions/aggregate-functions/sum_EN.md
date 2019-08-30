# SUM
## Description
### Syntax

` Sum (Expr)'


Used to return the sum of all values of the selected field

## example
```
MySQL > select sum(scan_rows) from log_statis group by datetime;
+------------------+
| sum(`scan_rows`) |
+------------------+
|       8217360135 |
+------------------+
```
##keyword
SUM
