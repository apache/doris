# dayofmonth
## description

获得日期中的天信息，返回值范围从1-31。

参数为Date或者Datetime类型

 Syntax

`INT DAYOFMONTH(DATETIME date)`

## example

```
mysql> select dayofmonth('1987-01-31');
+-----------------------------------+
| dayofmonth('1987-01-31 00:00:00') |
+-----------------------------------+
|                                31 |
+-----------------------------------+
##keyword
DAYOFMONTH,DAYOFMONTH
