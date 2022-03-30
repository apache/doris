--- 
{
    "title": "User Property",
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

# User configuration item

This document mainly introduces related configuration items at the User level. The configuration of the User level is mainly effective for a single user. Each user can set their own User property. Does not affect each other.

## View configuration items

After the FE is started, on the MySQL client, use the following command to view the User configuration items:

`SHOW PROPERTY [FOR user] [LIKE key pattern]`

The specific syntax can be queried through the command: `help show property;`.

## Set configuration items

After FE is started, on the MySQL client, modify the User configuration items with the following command:

`SET PROPERTY [FOR'user'] 'key' = 'value' [,'key' ='value']`

The specific syntax can be queried through the command: `help set property;`.

User-level configuration items will only take effect for the specified users, and will not affect the configuration of other users.

## Application examples

1. Modify the max_user_connections of user Billie

    Use `SHOW PROPERTY FOR 'Billie' LIKE '%max_user_connections%';` to check that the current maximum number of links for Billie users is 100.

    Use `SET PROPERTY FOR 'Billie' 'max_user_connections' = '200';` to modify the current maximum number of connections for Billie users to 200.

## Configuration item list

### max_user_connections

    The maximum number of user connections, the default value is 100 In general, this parameter does not need to be changed unless the number of concurrent queries exceeds the default value.

### max_query_instances

    The maximum number of instances that the user can use at a certain point in time, The default value is -1, negative number means use default_max_query_instances config.

### resource

### quota

### default_load_cluster

### load_cluster
