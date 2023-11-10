---
{
"title": "ALTER-COLOCATE-GROUP",
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

## ALTER-COLOCATE-GROUP

### Name

ALTER COLOCATE GROUP 

<version since="dev"></version>

### Description

该语句用于修改 Colocation Group 的属性。

语法：

```sql
ALTER COLOCATE GROUP  [database.]group
SET (
    property_list
);
```

注意：

1. 如果colocate group是全局的，即它的名称是以 `__global__` 开头的，那它不属于任何一个Database；

2. property_list 是colocation group属性，目前只支持修改`replication_num` 和 `replication_allocation`。
    修改colocation group的这两个属性修改之后，同时把该group的表的属性`default.replication_allocation` 、
    属性`dynamic.replication_allocation `、以及已有分区的`replication_allocation`改成跟它一样。



### Example

1. 修改一个全局group的副本数

    ```sql
    # 建表时设置 "colocate_with" = "__global__foo"
    
    ALTER COLOCATE GROUP __global__foo
    SET (
        "replication_num"="1"
    );
    ```

2. 修改一个非全局group的副本数

 ```sql
    # 建表时设置 "colocate_with" = "bar"，且表属于Database example_db
    
    ALTER COLOCATE GROUP example_db.bar
    SET (
        "replication_num"="1"
    );
    ```

### Keywords

```sql
ALTER, COLOCATE , GROUP
```

### Best Practice
