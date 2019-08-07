# month
## description
### Syntax

`INT MONTH(DATETIME date)`


返回时间类型中的月份信息，范围是1, 12

参数为Date或者Datetime类型

## example

```
mysql> select month('1987-01-01');
+-----------------------------+
| month('1987-01-01 00:00:00') |
+-----------------------------+
|                           1 |
+-----------------------------+
##keyword
MONTH
