Replace regexp
Description
'35;'35;' 35; Syntax

VARCHAR regexp replace (VARCHAR str, VARCHAR pattern, VARCHAR repl)


Regular matching of STR strings, replacing the part hitting pattern with repl

'35;'35; example

```
mysql> SELECT regexp_replace('a b c', " ", "-");
+-----------------------------------+
| regexp_replace('a b c', ' ', '-') |
+-----------------------------------+
A -b -c `124;
+-----------------------------------+

mysql> SELECT regexp_replace('a b c','(b)','<\\1>');
+----------------------------------------+
Replace ('a b c','(b)','<\1 >') regexp;
+----------------------------------------+
A <b >c {1244}
+----------------------------------------+
```
##keyword
REGEXP_REPLACE,REGEXP,REPLACE
