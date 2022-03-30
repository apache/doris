---
{
    "title": "DROP TABLE",
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

# DROP TABLE
## description
    该语句用于删除 table 。
    语法：
        DROP TABLE [IF EXISTS] [db_name.]table_name [FORCE];
        
    说明：
        1) 执行 DROP TABLE 一段时间内，可以通过 RECOVER 语句恢复被删除的表。详见 RECOVER 语句
        2) 如果执行 DROP TABLE FORCE，则系统不会检查该表是否存在未完成的事务，表将直接被删除并且不能被恢复，一般不建议执行此操作
        
## example
    1. 删除一个 table
        DROP TABLE my_table;
        
    2. 如果存在，删除指定 database 的 table
        DROP TABLE IF EXISTS example_db.my_table;

## keyword
    DROP,TABLE
    
