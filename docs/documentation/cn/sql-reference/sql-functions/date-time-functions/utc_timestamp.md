# utc_timestamp

## Syntax

`DATETIME UTC_TIMESTAMP()`

## Description

返回当前UTC日期和时间在 "YYYY-MM-DD HH:MM:SS" 或

"YYYYMMDDHHMMSS"格式的一个值

根据该函数是否用在字符串或数字语境中

## Examples

```
mysql> select utc_timestamp(),utc_timestamp() + 1;
+---------------------+---------------------+
| utc_timestamp()     | utc_timestamp() + 1 |
+---------------------+---------------------+
| 2019-07-10 12:31:18 |      20190710123119 |
+---------------------+---------------------+
```