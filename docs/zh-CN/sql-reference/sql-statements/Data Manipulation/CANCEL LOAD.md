---
{
    "title": "CANCEL LOAD",
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

# CANCEL LOAD
## description

    该语句用于撤销指定 load label 的批次的导入作业。
    这是一个异步操作，任务提交成功则返回。执行后可使用 SHOW LOAD 命令查看进度。
    语法：
        CANCEL LOAD
        [FROM db_name]
        WHERE LABEL = "load_label";
        
## example

    1. 撤销数据库 example_db 上， label 为 example_db_test_load_label 的导入作业
        CANCEL LOAD
        FROM example_db
        WHERE LABEL = "example_db_test_load_label";
        
## keyword
    CANCEL,LOAD

