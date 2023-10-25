---
{
    "title": "FIELD",
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

## field

<version since="1.2">

field

</version>

### description
#### Syntax

`field(Expr e, param1, param2, param3,.....)`

在order by子句中，可以使用自定义排序，可以将expr中的数据按照指定的param1，2，3顺序排列。
不在param参数中的数据不会参与排序,将会放在最前面,可以使用asc，desc控制整体顺序。
如果有NULL值，可以使用nulls first，nulls last控制null的顺序


### example

```

mysql> select k1,k7 from baseall where k1 in (1,2,3) order by field(k1,2,1,3);
+------+------------+
| k1   | k7         |
+------+------------+
|    2 | wangyu14   |
|    1 | wangjing04 |
|    3 | yuanyuan06 |
+------+------------+

mysql> select class_name from class_test order by field(class_name,'Suzi','Ben','Henry');
+------------+
| class_name |
+------------+
| Suzi       |
| Suzi       |
| Ben        |
| Ben        |
| Henry      |
| Henry      |
+------------+

mysql> select class_name from class_test order by field(class_name,'Suzi','Ben','Henry') desc;
+------------+
| class_name |
+------------+
| Henry      |
| Henry      |
| Ben        |
| Ben        |
| Suzi       |
| Suzi       |
+------------+

mysql> select class_name from class_test order by field(class_name,'Suzi','Ben','Henry') nulls first;
+------------+
| class_name |
+------------+
| null       |
| Suzi       |
| Suzi       |
| Ben        |
| Ben        |
| Henry      |
| Henry      |
+------------+
```
### keywords
    field
