# SUM
## description

用于返回选中字段所有值的和

 Syntax

`SUM(expr)`

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
SUM,SUM
