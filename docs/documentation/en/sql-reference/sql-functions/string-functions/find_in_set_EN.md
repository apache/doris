IV35; Find@U set
Description
'35;'35;' 35; Syntax

"NOT found in set (VARCHAR str., VARCHAR strlist)"


Return to the location where the str first appears in strlist (counting from 1). Strlist is a comma-separated string. If not, return 0. Any parameter is NULL, returning NULL.

'35;'35; example

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
