---
{
    "title": "SHOW ROUTINE LOAD",
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

# SHOW ROUTINE LOAD
## example

1. 展示名称为 test1 的所有例行导入作业（包括已停止或取消的作业）。结果为一行或多行。

    SHOW ALL ROUTINE LOAD FOR test1;

2. 展示名称为 test1 的当前正在运行的例行导入作业

    SHOW ROUTINE LOAD FOR test1;

3. 显示 example_db 下，所有的例行导入作业（包括已停止或取消的作业）。结果为一行或多行。

    use example_db;
    SHOW ALL ROUTINE LOAD;

4. 显示 example_db 下，所有正在运行的例行导入作业

    use example_db;
    SHOW ROUTINE LOAD;

5. 显示 example_db 下，名称为 test1 的当前正在运行的例行导入作业

    SHOW ROUTINE LOAD FOR example_db.test1;

6. 显示 example_db 下，名称为 test1 的所有例行导入作业（包括已停止或取消的作业）。结果为一行或多行。

    SHOW ALL ROUTINE LOAD FOR example_db.test1;

## keyword
    SHOW,ROUTINE,LOAD

