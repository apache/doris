---
{
    "title": "SHOW GRANTS",
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

# SHOW GRANTS
## description
    
    该语句用于查看用户权限。
    
    语法：
        SHOW [ALL] GRANTS [FOR user_identity];
        
    说明：
        1. SHOW ALL GRANTS 可以查看所有用户的权限。
        2. 如果指定 user_identity，则查看该指定用户的权限。且该 user_identity 必须为通过 CREATE USER 命令创建的。
        3. 如果不指定 user_identity，则查看当前用户的权限。
    
        
## example

    1. 查看所有用户权限信息
   
        SHOW ALL GRANTS;
        
    2. 查看指定 user 的权限

        SHOW GRANTS FOR jack@'%';
        
    3. 查看当前用户的权限

        SHOW GRANTS;

## keyword

    SHOW, GRANTS
