---
{
    "title": "COUNT_BY_ENUM",
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

## COUNT_BY_ENUM

### description
#### Syntax

`count_by_enum(expr1, expr2, ... , exprN);`

将列中数据看作枚举值，统计每个枚举值的个数。返回各个列枚举值的个数，以及非 null 值的个数与 null 值的个数。

#### Arguments

`expr1` — 至少填写一个输入。值为字符串（STRING）类型的列。

##### Returned value

返回一个 JSONArray 字符串。

例如：
```json
[{
	"cbe": {
		"F": 100,
		"M": 99
	},
	"notnull": 199,
	"null": 1,
	"all": 200
}, {
	"cbe": {
		"20": 10,
		"30": 5,
		"35": 1
	},
	"notnull": 16,
	"null": 184,
	"all": 200
}, {
	"cbe": {
		"北京": 10,
		"上海": 9,
		"广州": 20,
		"深圳": 30
	},
	"notnull": 69,
	"null": 131,
	"all": 200
}]
```
说明：返回值为一个 JSON array 字符串，内部对象的顺序是输入参数的顺序。
* cbe：根据枚举值统计非 null 值的统计结果
* notnull：非 null 的个数
* null：null 值个数
* all：总数，包括 null 值与非 null 值

### example

```sql
DROP TABLE IF EXISTS count_by_enum_test;

CREATE TABLE count_by_enum_test(
                `id` varchar(1024) NULL,
                `f1` text REPLACE_IF_NOT_NULL NULL,
                `f2` text REPLACE_IF_NOT_NULL NULL,
                `f3` text REPLACE_IF_NOT_NULL NULL
                )
AGGREGATE KEY(`id`)
DISTRIBUTED BY HASH(id) BUCKETS 3 
PROPERTIES ( 
    "replication_num" = "1"
); 

INSERT into count_by_enum_test (id, f1, f2, f3) values
                                        (1, "F", "10", "北京"),
                                        (2, "F", "20", "北京"),
                                        (3, "M", NULL, "上海"),
                                        (4, "M", NULL, "上海"),
                                        (5, "M", NULL, "广州");

SELECT * from count_by_enum_test;

+------+------+------+--------+
| id   | f1   | f2   | f3     |
+------+------+------+--------+
| 2    | F    | 20   | 北京   |
| 3    | M    | NULL | 上海   |
| 4    | M    | NULL | 上海   |
| 5    | M    | NULL | 广州   |
| 1    | F    | 10   | 北京   |
+------+------+------+--------+

select count_by_enum(f1) from count_by_enum_test;

+------------------------------------------------------+
| count_by_enum(`f1`)                                  |
+------------------------------------------------------+
| [{"cbe":{"M":3,"F":2},"notnull":5,"null":0,"all":5}] |
+------------------------------------------------------+

select count_by_enum(f2) from count_by_enum_test;

+--------------------------------------------------------+
| count_by_enum(`f2`)                                    |
+--------------------------------------------------------+
| [{"cbe":{"10":1,"20":1},"notnull":2,"null":3,"all":5}] |
+--------------------------------------------------------+

select count_by_enum(f1,f2,f3) from count_by_enum_test;

+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| count_by_enum(`f1`, `f2`, `f3`)                                                                                                                                                   |
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| [{"cbe":{"M":3,"F":2},"notnull":5,"null":0,"all":5},{"cbe":{"20":1,"10":1},"notnull":2,"null":3,"all":5},{"cbe":{"广州":1,"上海":2,"北京":2},"notnull":5,"null":0,"all":5}]       |
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

```

### keywords

COUNT_BY_ENUM
