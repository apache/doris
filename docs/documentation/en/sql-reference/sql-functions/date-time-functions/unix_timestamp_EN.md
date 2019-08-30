# unix_timestamp
## Description
### Syntax

`INT UNIX_TIMESTAMP(), UNIX_TIMESTAMP(DATETIME date)`


Converting a Date or Datetime type to a UNIX timestamp

If there are no parameters, the current time is converted into a timestamp

The parameter needs to be Date or Datetime type

## example

```
mysql> select unix_timestamp();
+------------------+
| unix_timestamp() |
+------------------+
|       1558589570 |
+------------------+

mysql> select unix_timestamp('2007-11-30 10:30:19');
+---------------------------------------+
| unix_timestamp('2007-11-30 10:30:19') |
+---------------------------------------+
|                            1196389819 |
+---------------------------------------+
##keyword
UNIX_TIMESTAMP,UNIX,TIMESTAMP
