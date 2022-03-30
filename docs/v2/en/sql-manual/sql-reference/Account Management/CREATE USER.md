---
{
    "title": "CREATE USER",
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

# CREATE USER
##Description

Syntax:

CREATE USER user_identity [IDENTIFIED BY 'password'] [DEFAULT ROLE 'role_name']

user_identity:
'user_name'@'host'

The CREATE USER command is used to create a Doris user. In Doris, a user_identity uniquely identifies a user. User_identity consists of two parts, user_name and host, where username is the user name. The host identifies the host address where the client connects. The host part can use% for fuzzy matching. If no host is specified, the default is'%', which means that the user can connect to Doris from any host.

The host part can also be specified as a domain with the grammar:'user_name'@['domain']. Even if surrounded by brackets, Doris will think of it as a domain and try to parse its IP address. At present, it only supports BNS analysis within Baidu.

If a role (ROLE) is specified, the permissions that the role has are automatically granted to the newly created user. If not specified, the user defaults to having no permissions. The specified ROLE must already exist.

## example

1. Create a passwordless user (without specifying host, it is equivalent to Jack @'%')

CREATE USER 'jack';

2. Create a password user that allows login from'172.10.1.10'

CREATE USER jack@'172.10.1.10' IDENTIFIED BY '123456';

3. To avoid passing plaintext, use case 2 can also be created in the following way

CREATE USER jack@'172.10.1.10' IDENTIFIED BY PASSWORD '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';

Later encrypted content can be obtained through PASSWORD (), for example:

SELECT PASSWORD('123456');

4. Create a user who is allowed to log in from the `192.168` subnet and specify its role as example_role

CREATE USER 'jack'@'192.168.%' DEFAULT ROLE 'example_role';

5. Create a user who is allowed to log in from the domain name 'example_domain'.

CREATE USER 'jack'@['example_domain'] IDENTIFIED BY '12345';

6. Create a user and specify a role

CREATE USER 'jack'@'%' IDENTIFIED BY '12345' DEFAULT ROLE 'my_role';

## keyword
CREATE, USER
