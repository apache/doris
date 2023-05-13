---
{
"title": "MAP",
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

## MAP

### name

<version since="1.2.0">

MAP

</version>

### description

`MAP<T1, T2>`

由T1, T2 类型元素组成的KV，不能作为key列使用。目前支持在Duplicate，Unique 模型的表中使用。

T支持的类型有：

```
BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, DECIMAL, DATE,
DATEV2, DATETIME, DATETIMEV2, CHAR, VARCHAR, STRING
```

### example

建表示例如下：

```
 CREATE TABLE IF NOT EXISTS simple_map (
              `id` INT(11) NULL COMMENT "",
              `m` Map<STRING, INT> NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            )
```

插入数据示例：

```
mysql> INSERT INTO simple_map VALUES(1, {'a': 100, 'b': 200});
```

查询数据示例：

```
mysql> SELECT * FROM simple_map;
+------+-----------------------------+
| id   | m                           |
+------+-----------------------------+
|    1 | {'a':100, 'b':200}          |
|    2 | {'b':100, 'c':200, 'd':300} |
|    3 | {'a':10, 'd':200}           |
+------+-----------------------------+
```

查询 map 列示例：

```
mysql> SELECT m FROM simple_map;
+-----------------------------+
| m                           |
+-----------------------------+
| {'a':100, 'b':200}          |
| {'b':100, 'c':200, 'd':300} |
| {'a':10, 'd':200}           |
+-----------------------------+
```

map 取值示例：

```
mysql> SELECT m['a'] FROM simple_map;
+-----------------------------+
| %element_extract%(`m`, 'a') |
+-----------------------------+
|                         100 |
|                        NULL |
|                          10 |
+-----------------------------+
```


### keywords

    MAP
