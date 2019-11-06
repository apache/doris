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

# CANCEL ALTER
## description
    该语句用于撤销一个 ALTER 操作。
    1. 撤销 ALTER TABLE COLUMN 操作
    语法：
        CANCEL ALTER TABLE COLUMN
        FROM db_name.table_name
    
    2. 撤销 ALTER TABLE ROLLUP 操作
    语法：
        CANCEL ALTER TABLE ROLLUP
        FROM db_name.table_name
        
    2. 撤销 ALTER CLUSTER 操作
    语法：
        （待实现...）

        
## example
    [CANCEL ALTER TABLE COLUMN]
    1. 撤销针对 my_table 的 ALTER COLUMN 操作。
        CANCEL ALTER TABLE COLUMN
        FROM example_db.my_table;
    
    [CANCEL ALTER TABLE ROLLUP]
    1. 撤销 my_table 下的 ADD ROLLUP 操作。
        CANCEL ALTER TABLE ROLLUP
        FROM example_db.my_table;

## keyword
    CANCEL,ALTER,TABLE,COLUMN,ROLLUP
    
