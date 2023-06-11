---
{
    "title": "ALTER-MATERIALIZED",
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

## ALTER-MATERIALIZED

### Name

ALTER MATERIALIZED VIEW

### Description

该语句用可以变更多表物化视图的刷新策略

语法：

```sql
ALTER MATERIALIZED VIEW mv_name mv_refersh_info;
```

说明：

1. mv_name： 待变更的物化视图的名称。
2. mv_refersh_info：新的刷新机制，详细信息请见[创建物化视图参数](../Create/CREATE-MATERIALIZED-VIEW.md)

### Example

1. 设置指定物化视图为手动刷新
```sql
ALTER MATERIALIZED VIEW example_mv REFRESH COMPLETE ON DEMAND;
```

2. 设置指定物化视图间隔两天全量刷新一次
```sql
alter MATERIALIZED VIEW example_mv REFRESH COMPLETE start with "2022-11-03 00:00:00" next 2 DAY
```

### Keywords

```text
ALTER,MATERIALIZED,VIEW
```

### Best Practice
