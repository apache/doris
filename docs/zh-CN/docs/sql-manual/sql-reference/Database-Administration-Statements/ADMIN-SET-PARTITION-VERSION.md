---
{
    "title": "ADMIN-SET-PARTITION-VERSION",
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

## ADMIN-SET-PARTITION-VERSION

### Name

ADMIN SET PARTITION VERSION

### Description

该语句用于手动改变指定分区的可见版本。

在某些特殊情况下，元数据中分区的版本有可能和实际副本的版本不一致，该命令可手动改变元数据中分区的版本。

语法：

```sql
ADMIN SET TABLE table_name PARTITION VERSION
        PROPERTIES ("key" = "value", ...);
```

目前支持如下属性：

1. "partition_id"：必需。指定一个 Partition Id.
2. "visible_version"：必需。指定 Version.

> 注意：
>
>  设置分区的版本需要先确认Be机器上实际副本的版本，此命令一般只用于紧急故障修复，请谨慎操作。

### Example

1. 设置 partition 1769152 在 FE 元数据上的版本为 100。

```sql
ADMIN SET TABLE tbl1 PARTITION VERSION PROPERTIES("partition_id" = "1769152", "visible_version" = "100");
```

### Keywords

    ADMIN, SET, PARTITION, VERSION
    
### Best Practice
