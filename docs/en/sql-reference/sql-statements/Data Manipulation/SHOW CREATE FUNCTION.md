---
{
    "title": "SHOW CREATE FUNCTION",
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

# SHOW CREATE FUNCTION
## description
    The statement is used to show the creation statement of user-defined function
    grammar:
        SHOW CREATE FUNTION function_name(arg_type [, ...]) [FROM db_name]];
        
    Description:
       `function_name`: the name of the function to be displayed
       `arg_type`: the parameter list of the function to be displayed
       If you do not specify db_name, use the current default db
        
## example
    1. Show the creation statement of the specified function under the default db
        SHOW CREATE FUNCTION my_add(INT, INT)
         
## keyword
    SHOW,CREATE,FUNCTION