# VAR_SAMP,VARIANCE_SAMP
## description
### Syntax

`VAR_SAMP(expr)`


返回expr表达式的样本方差

## example
```
MySQL > select var_samp(scan_rows) from log_statis group by datetime;
+-----------------------+
| var_samp(`scan_rows`) |
+-----------------------+
|    5.6227132145741789 |
+-----------------------+
##keyword
VAR_SAMP,VARIANCE_SAMP,VAR,SAMP,VARIANCE
