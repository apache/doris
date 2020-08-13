---
{
    "title": "SHOW PROPERTY",
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

# SHOW PROPERTY
## description
    该语句用于查看用户的属性
    语法：
        SHOW PROPERTY [FOR user] [LIKE key]

## example
    1. 查看 jack 用户的属性
        SHOW PROPERTY FOR 'jack'

    2. 查看 jack 用户导入cluster相关属性
        SHOW PROPERTY FOR 'jack' LIKE '%load_cluster%'

## keyword
    SHOW, PROPERTY
    
