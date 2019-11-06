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

# DROP USER
## Description

Syntax:

DROP USER 'user_name'

The DROP USER command deletes a Palo user. Doris does not support deleting the specified user_identity here. When a specified user is deleted, all user_identities corresponding to that user are deleted. For example, two users, Jack @'192%'and Jack @['domain'] were created through the CREATE USER statement. After DROP USER'jack' was executed, Jack @'192%'and Jack @['domain'] would be deleted.

## example

1. Delete user jack

DROP USER 'jack'

## keyword
DROP, USER
