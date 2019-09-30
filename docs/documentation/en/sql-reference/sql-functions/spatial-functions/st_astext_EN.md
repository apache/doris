# `ST_AsText`,`ST_AsWKT`
## Description
### Syntax

'VARCHAR ST'u AsText (GEOMETRY geo)'


Converting a geometric figure into a WKT (Well Known Text) representation

## example

```
mysql> SELECT ST_AsText(ST_Point(24.7, 56.7));
+---------------------------------+
124st text (st point (24.7, 56.7))124st text;
+---------------------------------+
| POINT (24.7 56.7)               |
+---------------------------------+
```
##keyword
ST_ASTEXT,ST_ASWKT,ST,ASTEXT,ASWKT
