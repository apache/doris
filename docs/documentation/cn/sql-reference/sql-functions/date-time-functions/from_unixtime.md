# from_unixtime

## Syntax

`DATETIME FROM_UNIXTIME(INT unix_timestamp[, VARCHAR string_format])`

## Description

将unix时间戳转化位对应的time格式，返回的格式由string_format指定

默认为yyyy-MM-dd HH:mm:ss

传入的是整形，返回的是字符串类型

目前string_format只支持两种类型的格式：yyyy-MM-dd，yyyy-MM-dd HH:mm:ss

其余string_format格式是非法的，返回NULL

## Examples

```
mysql> select from_unixtime(1196440219);
+---------------------------+
| from_unixtime(1196440219) |
+---------------------------+
| 2007-12-01 00:30:19       |
+---------------------------+

mysql> select from_unixtime(1196440219, 'yyyy-MM-dd');
+-----------------------------------------+
| from_unixtime(1196440219, 'yyyy-MM-dd') |
+-----------------------------------------+
| 2007-12-01                              |
+-----------------------------------------+

mysql> select from_unixtime(1196440219, 'yyyy-MM-dd HH:mm:ss');
+--------------------------------------------------+
| from_unixtime(1196440219, 'yyyy-MM-dd HH:mm:ss') |
+--------------------------------------------------+
| 2007-12-01 00:30:19                              |
+--------------------------------------------------+
```