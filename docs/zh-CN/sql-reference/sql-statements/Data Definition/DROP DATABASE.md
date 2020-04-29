---
{
    "title": "DROP DATABASE",
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

# DROP DATABASE
## description
    该语句用于删除数据库（database）
    语法：
        DROP DATABASE [IF EXISTS] db_name;

    说明：
        执行 DROP DATABASE 一段时间内，可以通过 RECOVER 语句恢复被删除的 database。详见 RECOVER 语句
        
## example
    1. 删除数据库 db_test
        DROP DATABASE db_test;
        
## keyword
    DROP,DATABASE
        
