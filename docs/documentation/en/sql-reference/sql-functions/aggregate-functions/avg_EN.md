#AVG
## Description
### Syntax

`AVG([DISTINCT] expr)`


Used to return the average value of the selected field

Optional field DISTINCT parameters can be used to return the weighted average

## example

```
mysql> SELECT datetime, AVG(cost_time) FROM log_statis group by datetime;
+---------------------+--------------------+
"1. Article 124b; Article 124b; Article 124g (`cost u time');
+---------------------+--------------------+
| 2019-07-03 21:01:20 | 25.827794561933533 |
+---------------------+--------------------+

mysql> SELECT datetime, AVG(distinct cost_time) FROM log_statis group by datetime;
+---------------------+---------------------------+
| datetime            | avg(DISTINCT `cost_time`) |
+---------------------+---------------------------+
| 2019-07-04 02:23:24 |        20.666666666666668 |
+---------------------+---------------------------+

```
##keyword
AVG
