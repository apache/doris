# day
## description
### Syntax

`INT DAY(DATETIME date)`


获得日期中的天信息，返回值范围从1-31。

参数为Date或者Datetime类型

## example

```
mysql> select day('1987-01-31');
+----------------------------+
| day('1987-01-31 00:00:00') |
+----------------------------+
|                         31 |
+----------------------------+
##keyword
DAY
