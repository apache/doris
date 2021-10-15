---
{
    "title": "SHOW SYNC JOB",
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

# SHOW SYNC JOB

## description

此命令用于当前显示所有数据库内的常驻数据同步作业状态。

语法：

	SHOW SYNC JOB [FROM db_name]

## example

1. 展示当前数据库的所有数据同步作业状态。

	SHOW SYNC JOB;
	
2. 展示数据库 `test_db` 下的所有数据同步作业状态。

	SHOW SYNC JOB FROM `test_db`;
	
## keyword

	SHOW,SYNC,JOB,BINLOG