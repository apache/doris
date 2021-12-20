---
{
    "title": "SET LDAP_ADMIN_PASSWORD",
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

# SET LDAP_ADMIN_PASSWORD
## description

Syntax:

    SET LDAP_ADMIN_PASSWORD = 'plain password'

    The SET command is used to set the LDAP administrator password. When using LDAP authentication, doris needs to use the administrator account and password to query the LDAP service for information about the logging user.

## example

1. Set LDAP admin password:
```
SET LDAP_ADMIN_PASSWORD = '123456'
```

## keyword
    SET, LDAP, LDAP_ADMIN_PASSWORD

