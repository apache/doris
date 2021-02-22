---
{
    "title": "SHOW CREATE FUNCTION",
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

# SHOW CREATE FUNCTION
## description
    该语句用于展示用户自定义函数的创建语句
    语法：
        SHOW CREATE FUNTION function_name(arg_type [, ...]) [FROM db_name]];
        
    说明：
       `function_name`: 要展示的函数名称
       `arg_type`: 要展示的函数的参数列表
       如果不指定 db_name，使用当前默认 db
        
## example
    1. 展示默认db下指定函数的创建语句
        SHOW CREATE FUNCTION my_add(INT, INT)
         
## keyword
    SHOW,CREATE,FUNCTION