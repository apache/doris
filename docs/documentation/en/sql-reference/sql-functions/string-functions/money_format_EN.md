# money_format
## Description
### Syntax

VARCHAR money format (Number)


The number is output in currency format, the integer part is separated by commas every three bits, and the decimal part is reserved for two bits.

## example

```
mysql> select money_format(17014116);
+------------------------+
| money_format(17014116) |
+------------------------+
| 17,014,116.00          |
+------------------------+

mysql> select money_format(1123.456);
+------------------------+
| money_format(1123.456) |
+------------------------+
| 1,123.46               |
+------------------------+

mysql> select money_format(1123.4);
+----------------------+
| money_format(1123.4) |
+----------------------+
| 1,123.40             |
+----------------------+
```
##keyword
MONEY_FORMAT,MONEY,FORMAT
