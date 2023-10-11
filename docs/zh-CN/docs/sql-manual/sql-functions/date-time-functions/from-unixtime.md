---
{
    "title": "FROM_UNIXTIME",
    "language": "zh-CN"
}
---

<!-- 
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

## from_unixtime
### description
#### Syntax

`DATETIME FROM_UNIXTIME(INT unix_timestamp[, VARCHAR string_format])`


将 unix 时间戳转化为对应的 time 格式，返回的格式由 `string_format` 指定

支持date_format中的format格式，默认为 %Y-%m-%d %H:%i:%s

传入的是整型，返回的是字符串类型

其余 `string_format` 格式是非法的，返回NULL


### example

```
mysql> select from_unixtime(1196440219);
+---------------------------+
| from_unixtime(1196440219) |
+---------------------------+
| 2007-12-01 00:30:19       |
+---------------------------+

mysql> select from_unixtime(1196440219, 'yyyy-MM-dd HH:mm:ss');
+--------------------------------------------------+
| from_unixtime(1196440219, 'yyyy-MM-dd HH:mm:ss') |
+--------------------------------------------------+
| 2007-12-01 00:30:19                              |
+--------------------------------------------------+

mysql> select from_unixtime(1196440219, '%Y-%m-%d');
+-----------------------------------------+
| from_unixtime(1196440219, '%Y-%m-%d') |
+-----------------------------------------+
| 2007-12-01                              |
+-----------------------------------------+

mysql> select from_unixtime(1196440219, '%Y-%m-%d %H:%i:%s');
+--------------------------------------------------+
| from_unixtime(1196440219, '%Y-%m-%d %H:%i:%s') |
+--------------------------------------------------+
| 2007-12-01 00:30:19                              |
+--------------------------------------------------+
```

对于超过范围的时间戳，可以采用from_second函数
`DATETIME FROM_SECOND(BIGINT unix_timestamp)`
```
mysql> select from_second(21474836470);
+--------------------------+
| from_second(21474836470) |
+--------------------------+
| 2650-07-06 16:21:10      |
+--------------------------+
```

### keywords

    FROM_UNIXTIME,FROM,UNIXTIME
