# MAX
## description

返回expr表达式的最大值

 Syntax

`MAX(expr)`

## example
```
MySQL > select max(scan_rows) from log_statis group by datetime;
+------------------+
| max(`scan_rows`) |
+------------------+
|          4671587 |
+------------------+
```
##keyword
MAX,MAX
