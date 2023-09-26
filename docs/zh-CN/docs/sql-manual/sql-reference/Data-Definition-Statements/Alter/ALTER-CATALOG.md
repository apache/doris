---
{
    "title": "ALTER-CATALOG",
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

## ALTER-CATALOG

### Name

<version since="1.2">

ALTER CATALOG

</version>

### Description

该语句用于设置指定数据目录的属性。（仅管理员使用）

1) 重命名数据目录

```sql
ALTER CATALOG catalog_name RENAME new_catalog_name;
```
注意：
- `internal` 是内置数据目录，不允许重命名
- 对 `catalog_name` 拥有 Alter 权限才允许对其重命名
- 重命名数据目录后，如需要，请使用 REVOKE 和 GRANT 命令修改相应的用户权限。

2) 设置数据目录属性

```sql
ALTER CATALOG catalog_name SET PROPERTIES ('key1' = 'value1' [, 'key' = 'value2']); 
```

更新指定属性的值为指定的 value。如果 SET PROPERTIES 从句中的 key 在指定 catalog 属性中不存在，则新增此 key。

注意：
- 不可更改数据目录类型，即 `type` 属性
- 不可更改内置数据目录 `internal` 的属性

3) 修改数据目录注释

```sql
ALTER CATALOG catalog_name MODIFY COMMENT "new catalog comment";
```

注意：
- `internal` 是内置数据目录，不允许修改注释

### Example

1. 将数据目录 ctlg_hive 重命名为 hive

```sql
ALTER CATALOG ctlg_hive RENAME hive;
```

3. 更新名为 hive 数据目录的属性 `hive.metastore.uris`

```sql
ALTER CATALOG hive SET PROPERTIES ('hive.metastore.uris'='thrift://172.21.0.1:9083');
```

4. 更改名为 hive 数据目录的注释

```sql
ALTER CATALOG hive MODIFY COMMENT "new catalog comment";
```

### Keywords

ALTER,CATALOG,RENAME,PROPERTY

### Best Practice
