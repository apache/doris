# regexp_extract
## Description
### Syntax

'VARCHAR regexp 'extract (VARCHAR str, VARCHAR pattern, int pos)


The string STR is matched regularly and the POS matching part which conforms to pattern is extracted. Patterns need to match exactly some part of the STR to return to the matching part of the pattern. If there is no match, return an empty string.

## example

```
mysql> SELECT regexp_extract('AbCdE', '([[:lower:]]+)C([[:lower:]]+)', 1);
+-------------------------------------------------------------+
.124; regexp Extract ('AbCdE', [[[[[:lower:]]]+)C ([[[:lower:]+]]]]'-1'-124;
+-------------------------------------------------------------+
(b)'1244;
+-------------------------------------------------------------+

mysql> SELECT regexp_extract('AbCdE', '([[:lower:]]+)C([[:lower:]]+)', 2);
+-------------------------------------------------------------+
.124; regexp Extract ('AbCdE', [[[[[:lower:]]]+)C ([[[:lower:]+]]]]]'-2'-124;
+-------------------------------------------------------------+
(d) 124d;
+-------------------------------------------------------------+
```
##keyword
REGEXP_EXTRACT,REGEXP,EXTRACT
