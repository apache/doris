# to_days
## description
### Syntax

`INT TO_DAYS(DATETIME date)`


返回date距离0000-01-01的天数

参数为Date或者Datetime类型

## example

```
mysql> select to_days('2007-10-07');
+-----------------------+
| to_days('2007-10-07') |
+-----------------------+
|                733321 |
+-----------------------+
##keyword
TO_DAYS,TO,DAYS
