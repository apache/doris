---
{
    "title": "ADMIN-SET-PARTITION-VERSION",
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

## ADMIN-SET-PARTITION-VERSION

### Name

ADMIN SET PARTITION VERSION

### Description

This statement is used to set the version of the specified partition.

In certain cases, the version of the partition in the metadata may not be consistent with the version of the actual replica. This command can manually set the version of the partition in the metadata.

grammar:

```sql
ADMIN SET TABLE table_name PARTITION VERSION
        PROPERTIES ("key" = "value", ...);
```

The following properties are currently supported:

1. "partition_id": Required. Specify a Partition Id.
2. "visible_version": Required. Specify Version.

> Note:
>
> It is necessary to first confirm the version of the actual replica on the Be before set the version of the partition. This command is generally only used for emergency troubleshooting, please proceed with caution.

### Example

1. Set the version of partition 1769152 to 100.

```sql
ADMIN SET TABLE tbl1 PARTITION VERSION PROPERTIES("partition_id" = "1769152", "visible_version" = "100");
```

### Keywords

    ADMIN, SET, PARTITION, VERSION
    
### Best Practice
