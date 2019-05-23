# unix_timestamp

## Syntax

`UNIX_TIMESTAMP(), UNIX_TIMESTAMP(date)`

## Description

```
将Date或者Datetime类型转化为unix时间戳
如果没有参数，则是将当前的时间转化为时间戳
参数需要是Date或者Datetime类型
```

## Examples

```
mysql> select unix_timestamp();
+------------------+
| unix_timestamp() |
+------------------+
|       1558589570 |
+------------------+

mysql> select unix_timestamp('2007-11-30 10:30:19');
+---------------------------------------+
| unix_timestamp('2007-11-30 10:30:19') |
+---------------------------------------+
|                            1196389819 |
+---------------------------------------+
```