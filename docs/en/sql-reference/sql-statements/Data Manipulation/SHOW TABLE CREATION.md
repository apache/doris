---
{
    "title": "SHOW TABLE CREATION",
    "language": "en"
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

    This statement is used to show the execution of the specified Iceberg Database table creation task
    Syntax.
        SHOW TABLE CREATION [FROM db_name] [LIKE table_name_wild];
        
    Description.
        1. Usage Notes
            1) If db_name is not specified, the current default db is used.
            2) If you use LIKE, it will match the table creation task with table_name_wild in the table name
        2. The meaning of each column
            1) Table: the name of the table to be created
            2) Status: the creation status of the table, `success`/`fail`.
            3）CreateTime: the time to perform the task of creating the table
            4) Error Msg: Error message of the failed table creation, or empty if it succeeds.

## example

    1. Show all the table creation tasks in the default Iceberg db
        SHOW TABLE CREATION;

        mysql> show table creation;
        +--------+---------+---------------------+----------------------------------------------------------+
        | Table  | Status  | Create Time         | Error Msg                                                |
        +--------+---------+---------------------+----------------------------------------------------------+
        | logs   | fail    | 2022-01-10 15:59:21 | Cannot convert Iceberg type[list<string>] to Doris type. |
        | logs_1 | success | 2022-01-10 15:59:21 |                                                          |
        +--------+---------+---------------------+----------------------------------------------------------+
    
    2. Show the table creation tasks in the specified Iceberg db
        SHOW TABLE CREATION FROM example_db;

        mysql> show table creation from iceberg_db;
        +--------+---------+---------------------+----------------------------------------------------------+
        | Table  | Status  | Create Time         | Error Msg                                                |
        +--------+---------+---------------------+----------------------------------------------------------+
        | logs   | fail    | 2022-01-10 15:59:21 | Cannot convert Iceberg type[list<string>] to Doris type. |
        | logs_1 | success | 2022-01-10 15:59:21 |                                                          |
        +--------+---------+---------------------+----------------------------------------------------------+
        
    3. Show table creation tasks for the specified Iceberg db with the string "log" in the table name
        SHOW TABLE CREATION FROM example_db LIKE '%log%';
        
        mysql> show table creation from iceberg_db like "%1";
        +--------+---------+---------------------+-----------+
        | Table  | Status  | Create Time         | Error Msg |
        +--------+---------+---------------------+-----------+
        | logs_1 | success | 2022-01-10 15:59:21 |           |
        +--------+---------+---------------------+-----------+

## keyword

    SHOW,TABLE CREATION
