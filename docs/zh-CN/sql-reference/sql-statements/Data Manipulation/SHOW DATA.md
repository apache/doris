---
{
    "title": "SHOW DATA",
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

# SHOW DATA
## description
    该语句用于展示数据量和副本数量
    语法：
        SHOW DATA [FROM db_name[.table_name]];
        
    说明：
        1. 如果不指定 FROM 子句，使用展示当前 db 下细分到各个 table 的数据量和副本数量
        2. 如果指定 FROM 子句，则展示 table 下细分到各个 index 的数据量和副本数量
        3. 如果想查看各个 Partition 的大小，请参阅 help show partitions

## example
    1. 展示默认 db 的各个 table 的数据量，副本数量，汇总数据量和汇总副本数量。
        SHOW DATA;
        
    2. 展示指定 db 的下指定表的细分数据量和副本数量
        SHOW DATA FROM example_db.table_name;

## keyword
    SHOW,DATA

