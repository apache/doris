---
{
    "title": "SHOW-FILE",
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

## SHOW-FILE

### Name

SHOW FILE

### Description

该语句用于展示一个 database 内创建的文件

语法：

```sql
SHOW FILE [FROM database];
```

说明：

```text
FileId:     文件ID，全局唯一
DbName:     所属数据库名称
Catalog:    自定义分类
FileName:   文件名
FileSize:   文件大小，单位字节
MD5:        文件的 MD5
```

### Example

1. 查看数据库 my_database 中已上传的文件

    ```sql
    SHOW FILE FROM my_database;
    ```

### Keywords

    SHOW, FILE

### Best Practice

