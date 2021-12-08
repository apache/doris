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
        
    Instructions.
        1) If db_name is not specified, the current default db is used
        2) If you use LIKE, it will match the table creation task with table_name_wild in the table name

## example

    1. Show all the table creation tasks in the default Iceberg db
        SHOW TABLE CREATION;
    
    2. Show the table creation tasks in the specified Iceberg db
        SHOW TABLE CREATION FROM example_db;
        
    3. Show table creation tasks for the specified Iceberg db with the string "log" in the table name
        SHOW TABLE CREATION FROM example_db LIKE '%log%';
        
## keyword

    SHOW,TABLE CREATION
