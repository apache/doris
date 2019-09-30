# datediff
## Description
### Syntax

'DATETIME DATEDIFF (DATETIME expr1,DATETIME expr2)'


Expr1 - expr2 is calculated and the result is accurate to the sky.

Expr1 and expr2 parameters are valid date or date/time expressions.

Note: Only the date part of the value participates in the calculation.

### example

```
MySQL > select DateDiff (CAST ('2007 -12 -31 23:59:59 'AS DATETIME), CAST (2007 -12 -30' AS DATETIME));
+-----------------------------------------------------------------------------------+
;datediff (CAST ('2007 -12 -31 23:59:59 'AS DATETIME), CAST ('2007 -12 -30' THE DATETIME)) 124;
+-----------------------------------------------------------------------------------+
|                                                                                 1 |
+-----------------------------------------------------------------------------------+

mysql > select datediff (CAST ('2010 -11 -30 23:59:59 'AS DATETIME), CAST ('2010 -12 -31' AS DATETIME));
+-----------------------------------------------------------------------------------+
124th; DateDiff (CAST ('2010 -11 -30 23:59:59 'AS DATETIME), CAST ('2010 -12 -31' THE DATETIME))
+-----------------------------------------------------------------------------------+
|                                                                               -31 |
+-----------------------------------------------------------------------------------+
```
##keyword
DATEDIFF
