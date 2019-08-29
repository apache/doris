# from_unixtime
## description
### syntax

`DATETIME FROM UNIXTIME (INT unix timestamp [, VARCHAR string format]`

Convert the UNIX timestamp to the corresponding time format of bits, and the format returned is specified by `string_format`

Input is an integer and return is a string type

Currently, `string_format` supports following formats：

    %Y: Year. 	eg. 2014，1900
    %m: Month. 	eg. 12，09
    %d: Day.    eg. 11，01
    %H: Hour.   eg. 23，01，12
    %i: Minute. eg. 05，11
    %s: Second. eg. 59，01

Default is `%Y-%m-%d %H:%i:%s`

Other `string_format` is illegal and will returns NULL.

## example

```
mysql> select from_unixtime(1196440219);
+---------------------------+
| from_unixtime(1196440219) |
+---------------------------+
| 2007-12-01 00:30:19       |
+---------------------------+

mysql> select from_unixtime(1196440219, '%Y-%m-%d');
+-----------------------------------------+
| from_unixtime(1196440219, '%Y-%m-%d')   |
+-----------------------------------------+
| 2007-12-01                              |
+-----------------------------------------+

mysql> select from_unixtime(1196440219, '%Y-%m-%d %H:%i:%s');
+--------------------------------------------------+
|From unixtime (1196440219,'%Y-%m-%d %H:%i:%s')    |
+--------------------------------------------------+
| 2007-12-01 00:30:19                              |
+--------------------------------------------------+

##keyword

    FROM_UNIXTIME,FROM,UNIXTIME
