# SUM
Description
'35;'35;' 35; Syntax

` Sum (Expr)'


Used to return the sum of all values of the selected field

'35;'35; example
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
