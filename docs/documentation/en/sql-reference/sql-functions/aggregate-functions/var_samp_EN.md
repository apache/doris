"35; VARIANCE SAMP, VARIANCE SAMP
Description
'35;'35;' 35; Syntax

'VAR SAMP (expr)'


Returns the sample variance of the expr expression

'35;'35; example
```
MySQL > select var_samp(scan_rows) from log_statis group by datetime;
+-----------------------+
| var_samp(`scan_rows`) |
+-----------------------+
|    5.6227132145741789 |
+-----------------------+
##keyword
VAR SAMP, VARIANCE SAMP,VAR,SAMP,VARIANCE
