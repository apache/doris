---
{
    "title": "DROP-USER",
    "language": "zh-CN"
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

## DROP-USER

### Name

DROP USER

### Description

删除一个用户

```sql
 DROP USER 'user_identity'

    `user_identity`:
    
        user@'host'
        user@['domain']
```

 删除指定的 user identitiy.

### Example

1. 删除用户 jack@'192.%'

    ```sql
    DROP USER 'jack'@'192.%'
    ```

### Keywords

    DROP, USER

### Best Practice

