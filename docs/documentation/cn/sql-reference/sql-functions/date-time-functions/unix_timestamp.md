# unix_timestamp
## description

将Date或者Datetime类型转化为unix时间戳 

如果没有参数，则是将当前的时间转化为时间戳

参数需要是Date或者Datetime类型

 Syntax

`INT UNIX_TIMESTAMP(), UNIX_TIMESTAMP(DATETIME date)`

## example

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
##keyword
UNIX_TIMESTAMP,UNIX,TIMESTAMP
