---
{
    "title": "SET-PASSWORD",
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

## SET-PASSWORD

### Name

SET PASSWORD

### Description

The SET PASSWORD command can be used to modify a user's login password. If the [FOR user_identity] field does not exist, then change the current user's password

```sql
SET PASSWORD [FOR user_identity] =
    [PASSWORD('plain password')]|['hashed password']
````

Note that the user_identity here must exactly match the user_identity specified when creating a user with CREATE USER, otherwise an error will be reported that the user does not exist. If user_identity is not specified, the current user is 'username'@'ip', which may not match any user_identity. Current users can be viewed through SHOW GRANTS.

The plaintext password is input in the PASSWORD() method; when using a string directly, the encrypted password needs to be passed.
To modify the passwords of other users, administrator privileges are required.

### Example

1. Modify the current user's password

   ```sql
   SET PASSWORD = PASSWORD('123456')
   SET PASSWORD = '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9'
   ````

2. Modify the specified user password

   ```sql
   SET PASSWORD FOR 'jack'@'192.%' = PASSWORD('123456')
   SET PASSWORD FOR 'jack'@['domain'] = '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9'
   ````

### Keywords

    SET, PASSWORD

### Best Practice

