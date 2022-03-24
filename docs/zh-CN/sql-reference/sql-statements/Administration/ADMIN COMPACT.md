---
{
    "title": "ADMIN COMPACT",
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

# ADMIN COMPACT
## description

    该语句用于对指定表分区下的所有副本触发一次Compaction

    语法：

        ADMIN COMPACT TABLE table_name PARTITION partition_name WHERE TYPE='BASE/CUMULATIVE'

    说明：
    
        1. 该语句仅表示让系统尝试将分区下每一个副本的compaction任务提交给compaction线程池，并不保证每一个副本的compaction任务都能成功执行。
        2. 该语句每次只支持对表下的单个分区执行compaction。

## example

    1. 对指定分区下的所有副本触发一次cumulative compaction

        ADMIN COMPACT TABLE tbl PARTITION par01 WHERE TYPE='CUMULATIVE';

    2. 对指定分区下的所有副本触发一次base compaction

        ADMIN COMPACT TABLE tbl PARTITION par01 WHERE TYPE='BASE';
        
## keyword
    ADMIN,COMPACT

