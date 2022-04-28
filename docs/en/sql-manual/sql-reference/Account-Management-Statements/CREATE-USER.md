---
{
    "title": "CREATE-USER",
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

## CREATE-USER

### Name

CREATE USER

### Description

The CREATE USER command is used to create a Doris user.

```sql
CREATE USER user_identity [IDENTIFIED BY 'password'] [DEFAULT ROLE 'role_name']

    user_identity:
        'user_name'@'host'
````

In Doris, a user_identity uniquely identifies a user. user_identity consists of two parts, user_name and host, where username is the username. host Identifies the host address where the client connects. The host part can use % for fuzzy matching. If no host is specified, it defaults to '%', which means the user can connect to Doris from any host.

The host part can also be specified as a domain, the syntax is: 'user_name'@['domain'], even if it is surrounded by square brackets, Doris will think this is a domain and try to resolve its ip address. Currently, only Baidu's internal BNS resolution is supported.

If a role (ROLE) is specified, the newly created user will be automatically granted the permissions of the role. If not specified, the user has no permissions by default. The specified ROLE must already exist.

### Example

1. Create a passwordless user (if host is not specified, it is equivalent to jack@'%')

   ```sql
   CREATE USER 'jack';
   ````

2. Create a user with a password to allow login from '172.10.1.10'

   ```sql
   CREATE USER jack@'172.10.1.10' IDENTIFIED BY '123456';
   ````

3. In order to avoid passing plaintext, use case 2 can also be created in the following way

   ```sql
   CREATE USER jack@'172.10.1.10' IDENTIFIED BY PASSWORD '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';
   The encrypted content can be obtained through PASSWORD(), for example:
   SELECT PASSWORD('123456');
   ````

4. Create a user that is allowed to log in from the '192.168' subnet, and specify its role as example_role

   ```sql
   CREATE USER 'jack'@'192.168.%' DEFAULT ROLE 'example_role';
   ````

5. Create a user that is allowed to log in from the domain 'example_domain'

   ```sql
   CREATE USER 'jack'@['example_domain'] IDENTIFIED BY '12345';
   ````

6. Create a user and assign a role

   ```sql
   CREATE USER 'jack'@'%' IDENTIFIED BY '12345' DEFAULT ROLE 'my_role';
   ````

### Keywords

    CREATE, USER

### Best Practice

