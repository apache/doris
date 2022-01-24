---
{
    "title": "SHOW TABLE CREATION",
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

# SHOW TABLE CREATION

## Description

    该语句用于展示指定的 Iceberg Database 建表任务的执行情况
    语法：
        SHOW TABLE CREATION [FROM db_name] [LIKE table_name_wild];
        
    说明：
        1. 使用说明
            1) 如果不指定 db_name，使用当前默认 db
            2) 如果使用 LIKE，则会匹配表名中包含 table_name_wild 的建表任务
        2. 各列含义说明
            1) Database: 数据库名称
            2) Table：要创建表的名称
            3) Status：表的创建状态，`success`/`fail`
            4) CreateTime：执行创建该表任务的时间
            5) Error Msg：创建表失败的错误信息，如果成功，则为空。
## example

    1. 展示默认 Iceberg db 中所有的建表任务
        SHOW TABLE CREATION;

        mysql> show table creation ;
        +----------------------------+--------+---------+---------------------+----------------------------------------------------------+
        | Database                   | Table  | Status  | Create Time         | Error Msg                                                |
        +----------------------------+--------+---------+---------------------+----------------------------------------------------------+
        | default_cluster:iceberg_db | logs_1 | success | 2022-01-24 19:42:45 |                                                          |
        | default_cluster:iceberg_db | logs   | fail    | 2022-01-24 19:42:45 | Cannot convert Iceberg type[list<string>] to Doris type. |
        +----------------------------+--------+---------+---------------------+----------------------------------------------------------+
    
    2. 展示指定 Iceberg db 中的建表任务
        SHOW TABLE CREATION FROM example_db;

        mysql> show table creation from iceberg_db;
        +----------------------------+--------+---------+---------------------+----------------------------------------------------------+
        | Database                   | Table  | Status  | Create Time         | Error Msg                                                |
        +----------------------------+--------+---------+---------------------+----------------------------------------------------------+
        | default_cluster:iceberg_db | logs_1 | success | 2022-01-24 19:42:45 |                                                          |
        | default_cluster:iceberg_db | logs   | fail    | 2022-01-24 19:42:45 | Cannot convert Iceberg type[list<string>] to Doris type. |
        +----------------------------+--------+---------+---------------------+----------------------------------------------------------+
        
    3. 展示指定 Iceberg db 中的建表任务，表名中包含字符串 "log" 的任务
        SHOW TABLE CREATION FROM example_db LIKE '%log%';

        mysql> show table creation from iceberg_db like "%1";
        +----------------------------+--------+---------+---------------------+-----------+
        | Database                   | Table  | Status  | Create Time         | Error Msg |
        +----------------------------+--------+---------+---------------------+-----------+
        | default_cluster:iceberg_db | logs_1 | success | 2022-01-24 19:42:45 |           |
        +----------------------------+--------+---------+---------------------+-----------+
        
## keyword

    SHOW,TABLE CREATION

