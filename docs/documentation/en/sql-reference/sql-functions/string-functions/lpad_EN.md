# lpad
## Description
### Syntax

'VARCHAR lpad (VARCHAR str., INT len, VARCHAR pad)'


Returns a string of length len in str, starting with the initials. If len is longer than str, pad characters are added to STR until the length of the string reaches len. If len is less than str's length, the function is equivalent to truncating STR strings and returning only strings of len's length.

## example

```
mysql > SELECT lpad ("hi", 5, "xy");
+---------------------+
1244; lpad ('hi', 5,'xy') 1244;
+---------------------+
| xyxhi               |
+---------------------+

mysql > SELECT lpad ("hi", 1, "xy");
+---------------------+
1244; lpad ('hi', 1,'xy') 1244;
+---------------------+
1.2.2.2.2.2.1.1.1.1.2.
+---------------------+
```
##keyword
LPAD
