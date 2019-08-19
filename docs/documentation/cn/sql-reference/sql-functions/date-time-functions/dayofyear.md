# dayofyear
## description
### Syntax

`INT DAYOFYEAR(DATETIME date)`


获得日期中对应当年中的哪一天。

参数为Date或者Datetime类型

## example

```
mysql> select dayofyear('2007-02-03 00:00:00');
+----------------------------------+
| dayofyear('2007-02-03 00:00:00') |
+----------------------------------+
|                               34 |
+----------------------------------+
```

##keyword

    DAYOFYEAR
