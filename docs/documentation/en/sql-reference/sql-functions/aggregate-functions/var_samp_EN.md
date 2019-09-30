# VARIANCE_SAMP,VARIANCE_SAMP
## Description
### Syntax

'VAR SAMP (expr)'


Returns the sample variance of the expr expression

## example
```
MySQL > select var_samp(scan_rows) from log_statis group by datetime;
+-----------------------+
| var_samp(`scan_rows`) |
+-----------------------+
|    5.6227132145741789 |
+-----------------------+
##keyword
VAR SAMP, VARIANCE SAMP,VAR,SAMP,VARIANCE
