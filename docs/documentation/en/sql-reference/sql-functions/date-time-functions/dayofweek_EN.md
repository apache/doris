# dayofweek
Description
'35;'35;' 35; Syntax

INT DayOfWeek (DATETIME date)


The DAYOFWEEK function returns the index value of the working day of the date, that is, 1 on Sunday, 2 on Monday, and 7 on Saturday.

The parameter is Date or Datetime type

'35;'35; example

```
mysql> select dayofweek('2019-06-25');
+----------------------------------+
Dayofweek ('2019 -06 -25 00:00:00') 124s;
+----------------------------------+
|                                3 |
+----------------------------------+
##keyword
DAYOFWEEK
