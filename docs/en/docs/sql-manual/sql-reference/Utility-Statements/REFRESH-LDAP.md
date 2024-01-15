---
{
    "title": "REFRESH-LDAP",
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

## REFRESH-LDAP

### Name

<version since="dev">

REFRESH-LDAP

</version>

### Description

This statement is used to refresh the cached information of LDAP in Doris. The default timeout for LDAP information cache in Doris is 12 hours, which can be viewed by `SHOW FRONTEND CONFIG LIKE 'ldap_ user_cache_timeout_s';`.

syntax:

```sql
REFRESH LDAP ALL;
REFRESH LDAP [for user_name];
```

### Example

1. refresh all LDAP user cache information

    ```sql
    REFRESH LDAP ALL;
    ```

2. refresh the cache information for the current LDAP user

    ```sql
    REFRESH LDAP;
    ```

3. refresh the cache information of the specified LDAP user user1

    ```sql
    REFRESH LDAP for user1;
    ```


### Keywords

REFRESH, LDAP

### Best Practice
