'35; from unixtime
Description
'35;'35;' 35; Syntax

'DATETIME FROM UNIXTIME (INT unix timestamp [, VARCHAR string format]'


Convert the UNIX timestamp to the corresponding time format of bits, and the format returned is specified by string_format

Default yyyy-MM-dd HH:mm:ss

Input is an integer and return is a string type

Currently string_format supports only two types of formats: yyyy-MM-dd, yyyy-MM-dd HH: mm:ss.

The rest of the string_format format is illegal and returns NULL

'35;'35; example

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
From unixtime (1196440219,'yyyy -MM -dd HH:mm:ss')
+--------------------------------------------------+
| 2007-12-01 00:30:19                              |
+--------------------------------------------------+
##keyword
FROM_UNIXTIME,FROM,UNIXTIME
