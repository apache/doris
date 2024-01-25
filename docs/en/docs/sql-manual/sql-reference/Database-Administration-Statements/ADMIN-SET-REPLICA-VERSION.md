---
{
    "title": "ADMIN-SET-REPLICA-VERSION",
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

## ADMIN-SET-REPLICA-VERSION

### Name

ADMIN SET REPLICA VERSION

### Description

This statement is used to set the version, maximum success version, and maximum failure version of the specified replica.

This command is currently only used to manually repair the replica version when the program is abnormal, so that the replica can recover from the abnormal state.

grammar:

```sql
ADMIN SET REPLICA VERSION
        PROPERTIES ("key" = "value", ...);
```

The following properties are currently supported:

1. `tablet_id`: Required. Specify a Tablet Id.
2. `backend_id`: Required. Specify Backend Id.
3. `version`: Optional. Set the replica version.
4. `last_success_version`: Optional. Set the replica max success version.
5. `last_failed_version`: Optional. Set the replica max failed version.

If the specified replica does not exist, it will be ignored.

> Note:
>
> Modifying these values ​​may cause subsequent data reading and writing failures, resulting in data inconsistency. Please operate with caution!
>
> Record the original value before modifying it. After the modification is completed, verify the read and write of the table. If the read and write fail, please restore the original value! But recovery may fail!
>
> It is strictly prohibited to operate the tablet that is writing data!

### Example

 1. Clear the replica failed version of tablet 10003 on BE 10001.

       ```sql
ADMIN SET REPLICA VERSION PROPERTIES("tablet_id" = "10003", "backend_id" = "10001", "last_failed_version" = "-1");       
       ```

2. Set the replica status of tablet 10003 on BE 10001 to ok.

```sql
ADMIN SET REPLICA VERSION PROPERTIES("tablet_id" = "10003", "backend_id" = "10001", "version" = "1004");
```

### Keywords

    ADMIN, SET, REPLICA, VERSION

### Best Practice

