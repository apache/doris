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
            1) Database: the name of the database
            2) Table: the name of the table to be created
            3) Status: the creation status of the table, `success`/`fail`.
            4) CreateTime: the time to perform the task of creating the table
            5) Error Msg: Error message of the failed table creation, or empty if it succeeds.

## example

    1. Show all the table creation tasks in the default Iceberg db
        SHOW TABLE CREATION;

        mysql> show table creation;
        +----------------------------+--------+---------+---------------------+----------------------------------------------------------+
        | Database                   | Table  | Status  | Create Time         | Error Msg                                                |
        +----------------------------+--------+---------+---------------------+----------------------------------------------------------+
        | default_cluster:iceberg_db | logs_1 | success | 2022-01-24 19:42:45 |                                                          |
        | default_cluster:iceberg_db | logs   | fail    | 2022-01-24 19:42:45 | Cannot convert Iceberg type[list<string>] to Doris type. |
        +----------------------------+--------+---------+---------------------+----------------------------------------------------------+
    
    2. Show the table creation tasks in the specified Iceberg db
        SHOW TABLE CREATION FROM example_db;

        mysql> show table creation from iceberg_db;
        +----------------------------+--------+---------+---------------------+----------------------------------------------------------+
        | Database                   | Table  | Status  | Create Time         | Error Msg                                                |
        +----------------------------+--------+---------+---------------------+----------------------------------------------------------+
        | default_cluster:iceberg_db | logs_1 | success | 2022-01-24 19:42:45 |                                                          |
        | default_cluster:iceberg_db | logs   | fail    | 2022-01-24 19:42:45 | Cannot convert Iceberg type[list<string>] to Doris type. |
        +----------------------------+--------+---------+---------------------+----------------------------------------------------------+
        
    3. Show table creation tasks for the specified Iceberg db with the string "log" in the table name
        SHOW TABLE CREATION FROM example_db LIKE '%log%';
        
        mysql> show table creation from iceberg_db like "%1";
        +----------------------------+--------+---------+---------------------+-----------+
        | Database                   | Table  | Status  | Create Time         | Error Msg |
        +----------------------------+--------+---------+---------------------+-----------+
        | default_cluster:iceberg_db | logs_1 | success | 2022-01-24 19:42:45 |           |
        +----------------------------+--------+---------+---------------------+-----------+

## keyword

    SHOW,TABLE CREATION
