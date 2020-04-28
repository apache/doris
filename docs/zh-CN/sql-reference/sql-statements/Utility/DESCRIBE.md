---
{
    "title": "DESCRIBE",
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

# DESCRIBE
## description
    该语句用于展示指定 table 的 schema 信息
    语法：
        DESC[RIBE] [db_name.]table_name [ALL];

    说明：
        如果指定 ALL，则显示该 table 的所有 index(rollup) 的 schema

## example

1. 显示Base表Schema

    DESC table_name;

2. 显示表所有 index 的 schema

    DESC db1.table_name ALL;

## keyword

    DESCRIBE,DESC
