# dayofweek
## description
### Syntax

`INT dayofweek(DATETIME date)`


DAYOFWEEK函数返回日期的工作日索引值，即星期日为1，星期一为2，星期六为7

参数为Date或者Datetime类型

## example

```
mysql> select dayofweek('2019-06-25');
+----------------------------------+
| dayofweek('2019-06-25 00:00:00') |
+----------------------------------+
|                                3 |
+----------------------------------+
##keyword
DAYOFWEEK
