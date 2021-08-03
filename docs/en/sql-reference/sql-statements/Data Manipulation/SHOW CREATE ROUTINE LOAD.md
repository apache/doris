---
{
"title": "SHOW CREATE ROUTINE LOAD",
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

# SHOW CREATE ROUTINE LOAD
## description
    The statement is used to show the routine load job creation statement of user-defined.

	The kafka partition and offset in the result show the currently consumed partition and the corresponding offset to be consumed.

    grammar：
        SHOW [ALL] CREATE ROUTINE LOAD for load_name;
        
    Description：
       `ALL`: optional，Is for getting all jobs, including history jobs
       `load_name`: routine load name

## example
    1. Show the creation statement of the specified routine load under the default db
        SHOW CREATE ROUTINE LOAD for test_load

## keyword
    SHOW,CREATE,ROUTINE,LOAD
