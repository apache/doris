---
{
    "title": "DROP VIEW",
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

# DROP VIEW
## Description
This statement is used to delete a logical view VIEW
Grammar:
DROP VIEW [IF EXISTS]
[db_name.]view_name;

## example
1. If it exists, delete view example_view on example_db
DROP VIEW IF EXISTS example_db.example_view;

## keyword
DROP,VIEW

