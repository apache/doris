# date_sub
## Description
### Syntax

`INT DATE_SUB(DATETIME date,INTERVAL expr type)`


Subtract the specified time interval from the date

The date parameter is a valid date expression.

The expr parameter is the interval you want to add.

Sweet, sweet, sweet

## example

```
mysql > select date sub ('2010 -11 -30 23:59:59', INTERVAL 2 DAY);
+-------------------------------------------------+
+ 124; date = USub (2010-11-30 23:59', interval 2 days);
+-------------------------------------------------+
| 2010-11-28 23:59:59                             |
+-------------------------------------------------+
##keyword
Date, date, date
