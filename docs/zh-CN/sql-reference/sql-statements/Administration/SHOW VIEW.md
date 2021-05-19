---
{
    "title": "SHOW DELETE",
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

# SHOW VIEW
## description
    该语句用于展示基于给定表建立的所有视图
    语法：
        SHOW VIEW { FROM | IN } table [ FROM db ]

## example
    1. 展示基于表 testTbl 建立的所有视图 view
        SHOW VIEW FROM testTbl;
        
## keyword
    SHOW,VIEW
    
