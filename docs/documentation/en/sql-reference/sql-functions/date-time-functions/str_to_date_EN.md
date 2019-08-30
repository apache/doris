# Str_to_date
## Description
### Syntax

'DATETIME STR TWO DATES (VARCHAR STR, VARCHAR format)'


Convert STR to DATE type by format specified, if the conversion result does not return NULL

The format format supported is consistent with date_format

## example

```
mysql > select str to u date ('2014 -12 -21 12:34:56','%Y -%m -%d%H:%i:%s');
+---------------------------------------------------------+
Date to date ('2014 -12 -21 12:34:56','%Y -%m -%d%H:%i:%s')
+---------------------------------------------------------+
| 2014-12-21 12:34:56                                     |
+---------------------------------------------------------+

mysql> select str_to_date('200442 Monday', '%X%V %W');
+-----------------------------------------+
Date to date ('200442 Monday','%X%V%W')
+-----------------------------------------+
| 2004-10-18                              |
+-----------------------------------------+
##keyword
STR_TO_DATE,STR,TO,DATE
