# date_format
Description
'35;'35;' 35; Syntax

'WARCHAR DATE'U FORMAT (DATETIME DATE, WARCHAR Format)'


Convert the date type to a bit string according to the format type.
Currently supports a string with a maximum 128 bytes and returns NULL if the length of the return value exceeds 128

The date parameter is the valid date. Format specifies the date/time output format.

The formats available are:

% a | Abbreviation for Sunday Name

% B | Abbreviated Monthly Name

% C | Month, numerical value

% D | Sky in the Moon with English Prefix

% d | Monthly day, numerical value (00-31)

% e | Monthly day, numerical value (0-31)

% f | microseconds

% H | Hours (00-23)

% h | hour (01-12)

% I | Hours (01-12)

% I | min, numerical value (00-59)

% J | Days of Year (001-366)

% k | hours (0-23)

% L | Hours (1-12)

% M | Moon Name

% m | month, numerical value (00-12)

%p%124; AM%25110PM

% R | Time, 12 - hour (hh: mm: SS AM or PM)

% S | seconds (00-59)

% s | seconds (00-59)

% T | Time, 24 - hour (hh: mm: ss)

% U | Week (00-53) Sunday is the first day of the week

% U | Week (00 - 53) Monday is the first day of the week

% V | Week (01-53) Sunday is the first day of the week, and% X is used.

% v | Week (01 - 53) Monday is the first day of the week, and% x is used

% W | Sunday

% w | Weekly day (0 = Sunday, 6 = Saturday)

% X | Year, where Sunday is the first day of the week, 4 places, and% V use

% X | year, of which Monday is the first day of the week, 4 places, and% V

% Y | Year, 4

% Y | Year, 2

'35;'35; example

```
mysql > select date'u format ('2009 -10 -04 22:23:00','%W%M%Y');
+------------------------------------------------+
+ 124; Date = UFormat (-2009-10-04 22:23:00', w%M%Y);
+------------------------------------------------+
| Sunday October 2009                            |
+------------------------------------------------+

mysql > select date'u format ('2007 -10 -04 22:23:00','%H:%i:%s');
+------------------------------------------------+
+ 124; Date = UFormat (-2007-10-04 22:23:00', H:% I:% s));
+------------------------------------------------+
| 22:23:00                                       |
+------------------------------------------------+

mysql > select date'u format ('1900 -10 -04 22:23:00','%D%y%a%d%m%b%j');
+------------------------------------------------------------+
+ 124; Date = UFormat (+1900-10-04 22:23:00',%Y%A%D%M%B%J)
+------------------------------------------------------------+
+ 124; 4th 00 THU 04 10 Oct 277;
+------------------------------------------------------------+

mysql > select date'u format ('1997 -10 -04 22:23:00','%H%k%I%r%T%S%w');
+------------------------------------------------------------+
+ 124; Date = UFormat ("1997-10-04 22:23:00",%H%K%I%R%T%S%W") = 124;
+------------------------------------------------------------+
22,22,10,23:00 PM 22:23:00 00,6,1244;
+------------------------------------------------------------+

mysql > select date'u format ('1999 -01 -01 00:00','%X%V');
+---------------------------------------------+
Date of format ('1999 -01 -01 00:00','%X%V')
+---------------------------------------------+
| 1998 52                                     |
+---------------------------------------------+

mysql> select date_format('2006-06-01', '%d');
+------------------------------------------+
date (2006 -06 -01 00:00','%d') 124date;
+------------------------------------------+
| 01                                       |
+------------------------------------------+
```
##keyword
DATE_FORMAT,DATE,FORMAT
