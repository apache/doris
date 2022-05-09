---
{
    "title": "CREATE-TABLE-AS-SELECT",
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

## CREATE-TABLE-AS-SELECT

### Name

CREATE TABLE AS SELECT

### Description

该语句通过 Select 语句返回结果创建表结构，同时导入数据

语法：

```sql
CREATE TABLE table_name [( column_name_list )]
opt_engine opt_partition opt_properties KW_AS query_stmt
 ```

说明: 

- 暂时不支持`decimal`类型的字段
- 用户需要拥有来源表的`SELECT`权限和目标库的`CREATE`权限
- 创建表成功后，会进行数据导入，如果导入失败，将会删除表

### Example

1. 使用 select 语句中的字段名

    ```sql
    create table `test`.`select_varchar` 
    PROPERTIES(\"replication_num\" = \"1\") 
    as select * from `test`.`varchar_table`
    ```

2. 自定义字段名(需要与返回结果字段数量一致)
    ```sql
    create table `test`.`select_name`(user, testname, userstatus) 
    PROPERTIES(\"replication_num\" = \"1\") 
    as select vt.userId, vt.username, jt.status 
    from `test`.`varchar_table` vt join 
    `test`.`join_table` jt on vt.userId=jt.userId
    ```
   
### Keywords

    CREATE, TABLE, AS, SELECT

### Best Practice

