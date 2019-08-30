# concat
## Description
### Syntax

'VARCHAR concat (VARCHAR,...)'


Connect multiple strings and return NULL if any of the parameters is NULL

## example

```
mysql> select concat("a", "b");
+------------------+
*124concat ('a','b') 1244;
+------------------+
1.2.2.2.2.2.
+------------------+

mysql> select concat("a", "b", "c");
+-----------------------+
124concat ('a','b','c') 1244;
+-----------------------+
1.2.2.2.2.2.2.
+-----------------------+

mysql > select concat ("a", null, "c");
+------------------------+
124concat (a), NULL,'c')
+------------------------+
No. No. No.
+------------------------+
```
##keyword
CONCAT
