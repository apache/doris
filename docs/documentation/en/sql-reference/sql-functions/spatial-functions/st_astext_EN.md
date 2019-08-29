'35; 'ST AsText','ST AsWKT'
Description
'35;'35;' 35; Syntax

'VARCHAR ST'u AsText (GEOMETRY geo)'


Converting a geometric figure into a WKT (Well Known Text) representation

'35;'35; example

```
mysql> SELECT ST_AsText(ST_Point(24.7, 56.7));
+---------------------------------+
124st text (st point (24.7, 56.7))124st text;
+---------------------------------+
| POINT (24.7 56.7)               |
+---------------------------------+
```
##keyword
ST. ASTEXT, ST. ASWKT, ST, ASTEXT, ASWKT
