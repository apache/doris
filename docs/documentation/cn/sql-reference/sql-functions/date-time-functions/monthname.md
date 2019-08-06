# monthname
## description

返回日期对应的月份名字

参数为Date或者Datetime类型

 Syntax

`VARCHAR MONTHNAME(DATE)`

## example

```
mysql> select monthname('2008-02-03 00:00:00');
+----------------------------------+
| monthname('2008-02-03 00:00:00') |
+----------------------------------+
| February                         |
+----------------------------------+
##keyword
MONTHNAME,MONTHNAME
