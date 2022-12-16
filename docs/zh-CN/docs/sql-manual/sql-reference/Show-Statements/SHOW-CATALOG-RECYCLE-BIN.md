---
{
    "title": "SHOW-CATALOG-RECYCLE-BIN",
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

## SHOW-CATALOG-RECYCLE-BIN

### Name

<version since="1.2">

SHOW CATALOG RECYCLE BIN

</version>

### Description

该语句用于展示回收站中可回收的库,表或分区元数据信息

语法：

```sql
SHOW CATALOG RECYCLE BIN [ WHERE NAME [ = "name" | LIKE "name_matcher"] ]
```

说明：

```
各列含义如下：
        Type：                元数据类型:Database、Table、Partition
        Name：                元数据名称		
        DbId：                database对应的id
        TableId：             table对应的id
        PartitionId：         partition对应的id
        DropTime：            元数据放入回收站的时间
```


### Example

 1. 展示所有回收站元数据
    
      ```sql
       SHOW CATALOG RECYCLE BIN;
      ```

 2. 展示回收站中名称'test'的元数据
    
      ```sql
       SHOW CATALOG RECYCLE BIN WHERE NAME = 'test';
      ```

### Keywords

    SHOW, CATALOG RECYCLE BIN

### Best Practice

