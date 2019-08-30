# NDV
## Description
### Syntax

`NDV (expr)`


Returns an approximate aggregation function similar to the result of COUNT (DISTINCT col).

It combines COUNT and DISTINCT faster and uses fixed-size memory, so less memory can be used for columns with high cardinality.

## example
```
MySQL > select ndv(query_id) from log_statis group by datetime;
+-----------------+
| ndv(`query_id`) |
+-----------------+
| 17721           |
+-----------------+
```
##keyword
NDV
