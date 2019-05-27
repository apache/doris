# to_days

## Syntax

`INT TO_DAYS(DATETIME date)`

## Description

返回date距离0000-01-01的天数

参数为Date或者Datetime类型

## Examples

```
mysql> select to_days('2007-10-07');
+-----------------------+
| to_days('2007-10-07') |
+-----------------------+
|                733321 |
+-----------------------+
```