# find_in_set
## description
### Syntax

`INT find_in_set(VARCHAR str, VARCHAR strlist)`

"NOT found in set (VARCHAR str., VARCHAR strlist)"


Return to the location where the str first appears in strlist (counting from 1). Strlist is a comma-separated string. If not, return 0. Any parameter is NULL, returning NULL.

## example

```
mysql > select find in u set ("b", "a,b,c");
+---------------------------+
Find in set ('b','a,b,c') 1244;
+---------------------------+
|                         2 |
+---------------------------+
```
##keyword
FIND IN SET,FIND,IN,SET
