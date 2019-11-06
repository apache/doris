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

# SHOW ALTER
## Description
This statement is used to show the execution of various modification tasks currently under way.
Grammar:
SHOW ALTER [CLUSTER | TABLE [COLUMN | ROLLUP] [FROM db_name]];

Explain:
TABLE COLUMN：展示修改列的 ALTER 任务
TABLE ROLLUP: Shows the task of creating or deleting ROLLUP index
If db_name is not specified, use the current default DB
CLUSTER: Show the cluster operation related tasks (only administrators use! To be realized...

## example
1. Show the task execution of all modified columns of default DB
SHOW ALTER TABLE COLUMN;

2. Show the execution of tasks to create or delete ROLLUP index for specified DB
SHOW ALTER TABLE ROLLUP FROM example_db;

3. Show cluster operations related tasks (only administrators use! To be realized...
SHOW ALTER CLUSTER;

## keyword
SHOW,ALTER

