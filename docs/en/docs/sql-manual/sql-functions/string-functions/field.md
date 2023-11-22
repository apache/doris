---
{
    "title": "FIELD",
    "language": "en"
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

<version since="dev">

field

</version>

### description
#### Syntax

`field(Expr e, param1, param2, param3,.....)`


In the order by clause, you can use custom sorting to arrange the data in expr in the specified param1, 2, and 3 order.
The data not in the param parameter will not participate in sorting, but will be placed first. 
You can use asc and desc to control the overall order.
If there is a NULL value, you can use nulls first, nulls last to control the order of nulls.


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
