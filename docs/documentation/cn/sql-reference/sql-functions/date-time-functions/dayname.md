# dayname
## description
### Syntax

`VARCHAR DAYNAME(DATE)`


返回日期对应的日期名字

参数为Date或者Datetime类型

## example

```
mysql> select dayname('2007-02-03 00:00:00');
+--------------------------------+
| dayname('2007-02-03 00:00:00') |
+--------------------------------+
| Saturday                       |
+--------------------------------+
##keyword
DAYNAME
