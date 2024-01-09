---
{
    "title": "DROP-ASYNC-MATERIALIZED-VIEW",
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

## DROP-ASYNC-MATERIALIZED-VIEW

### Name

DROP ASYNC MATERIALIZED VIEW

### Description

该语句用于删除异步物化视图。

语法：

```sql
DROP MATERIALIZED VIEW (IF EXISTS)? mvName=multipartIdentifier
```


1. IF EXISTS:
        如果物化视图不存在，不要抛出错误。如果不声明此关键字，物化视图不存在则报错。

2. mv_name:
        待删除的物化视图的名称。必填项。

### Example

1. 删除表物化视图mv1

```sql
DROP MATERIALIZED VIEW mv1;
```
2.如果存在，删除指定 database 的物化视图

```sql
DROP MATERIALIZED VIEW IF EXISTS db1.mv1;
```

### Keywords

    DROP, ASYNC, MATERIALIZED, VIEW

### Best Practice

