# utc_timestamp
Description
'35;'35;' 35; Syntax

`DATETIME UTC_TIMESTAMP()`


Returns the current UTC date and time in "YYYY-MM-DD HH: MM: SS" or

A Value of "YYYYMMDDHMMSS" Format

Depending on whether the function is used in a string or numeric context

'35;'35; example

```
mysql> select utc_timestamp(),utc_timestamp() + 1;
+---------------------+---------------------+
| utc_timestamp()     | utc_timestamp() + 1 |
+---------------------+---------------------+
| 2019-07-10 12:31:18 |      20190710123119 |
+---------------------+---------------------+
##keyword
UTC_TIMESTAMP,UTC,TIMESTAMP
