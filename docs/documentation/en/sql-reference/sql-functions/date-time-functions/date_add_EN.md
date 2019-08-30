# date_add
## Description
### Syntax

`INT DATE_ADD(DATETIME date,INTERVAL expr type)`


Adds a specified time interval to the date.

The date parameter is a valid date expression.

The expr parameter is the interval you want to add.

Sweet, sweet, sweet

## example

```
mysql > select date to add ('2010 -11 -30 23:59:59', INTERVAL 2 DAY);
+-------------------------------------------------+
+ 124; Date = U Add (= 2010-11-30 23:59', interval 2 days)
+-------------------------------------------------+
| 2010-12-02 23:59:59                             |
+-------------------------------------------------+
##keyword
DATE_ADD,DATE,ADD
