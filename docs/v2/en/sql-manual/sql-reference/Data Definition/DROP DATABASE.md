---
{
    "title": "DROP DATABASE",
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

# DROP DATABASE
##Description
This statement is used to delete the database
Grammar:
DROP DATABASE [IF EXISTS] db_name;

Explain:
1) After executing DROP DATABASE for a period of time, the deleted database can be restored through the RECOVER statement. See RECOVER statement for details
2) If DROP DATABASE FORCE is executed, the system will not check whether the database has unfinished transactions, the database will be deleted directly and cannot be recovered, generally this operation is not recommended

## example
1. Delete database db_test
DROP DATABASE db_test;

## keyword
DROP,DATABASE

