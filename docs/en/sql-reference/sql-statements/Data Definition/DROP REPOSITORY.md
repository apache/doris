---
{
    "title": "DROP REPOSITORY",
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

# DROP REPOSITORY
## Description
This statement is used to delete a created warehouse. Only root or superuser users can delete the warehouse.
Grammar:
DROP REPOSITORY `repo_name`;

Explain:
1. Delete the warehouse, just delete the mapping of the warehouse in Palo, and do not delete the actual warehouse data. After deletion, you can map to the repository again by specifying the same broker and LOCATION.

## example
1. Delete the warehouse named bos_repo:
DROP REPOSITORY `bos_repo`;

## keyword
DROP, REPOSITORY
