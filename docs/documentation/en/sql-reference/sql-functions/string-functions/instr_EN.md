# instr
## Description
### Syntax

'INT INSR (WARCHAR STR, WARCHAR substrate)'


Returns the location where substr first appeared in str (counting from 1). If substr does not appear in str, return 0.

## example

```
mysql> select instr("abc", "b");
+-------------------+
124Insr ('abc','b') 124
+-------------------+
|                 2 |
+-------------------+

mysql> select instr("abc", "d");
+-------------------+
124Insr ('abc','d') 124
+-------------------+
|                 0 |
+-------------------+
```
##keyword
INSTR
