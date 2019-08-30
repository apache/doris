# MAX
## description
### Syntax

`MAX(expr)`


Returns the maximum value of an expr expression

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
MAX
