---
{
    "title": "ADMIN SHOW DATA SKEW",
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

# ADMIN SHOW DATA SKEW
## description

    该语句用于查看表或某个分区的数据倾斜情况。

    语法：

        ADMIN SHOW DATA SKEW FROM [db_name.]tbl_name [PARTITION (p1)];

    说明：

        1. 必须指定且仅指定一个分区。对于非分区表，分区名称同表名。
		2. 结果将展示指定分区下，各个分桶的数据量，以及每个分桶数据量在总数据量中的占比。
        
## example

    1. 查看表的数据倾斜情况

        ADMIN SHOW DATA SKEW FROM db1.test PARTITION(p1);

## keyword

    ADMIN,SHOW,DATA,SKEW

