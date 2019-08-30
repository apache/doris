# STDDEV_SAMP
## Description
### Syntax

'STDDEV SAMP (expr)'


Returns the sample standard deviation of the expr expression

## example
```
MySQL > select stddev_samp(scan_rows) from log_statis group by datetime;
+--------------------------+
| stddev_samp(`scan_rows`) |
+--------------------------+
|        2.372044195280762 |
+--------------------------+
```
##keyword
STDDEVu SAMP,STDDEV,SAMP
