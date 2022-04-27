---
{
    "title": "SHOW-RESOURCES",
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

## SHOW-RESOURCES

### Name

SHOW RESOURCES

### Description

该语句用于展示用户有使用权限的资源。普通用户仅能展示有使用权限的资源，root 或 admin 用户会展示所有的资源。

语法：

```sql
SHOW RESOURCES
[
  WHERE
  [NAME [ = "your_resource_name" | LIKE "name_matcher"]]
  [RESOURCETYPE = ["SPARK"]]
]
[ORDER BY ...]
[LIMIT limit][OFFSET offset];
```

说明：

1. 如果使用 NAME LIKE，则会匹配 RESOURCES 的 Name 包含 name_matcher 的Resource
2.  如果使用 NAME = ，则精确匹配指定的 Name
3. 如果指定了 RESOURCETYPE，则匹配对应的 Resrouce 类型
4. 可以使用 ORDER BY 对任意列组合进行排序
5. 如果指定了 LIMIT，则显示 limit 条匹配记录。否则全部显示
6. 如果指定了 OFFSET，则从偏移量 offset 开始显示查询结果。默认情况下偏移量为0。

### Example

1. 展示当前用户拥有权限的所有 Resource
    
    ```sql
    SHOW RESOURCES;
    ```

1. 展示指定 Resource ，NAME 中包含字符串 "20140102"，展示10个属性
    
    ```sql
    SHOW RESOURCES WHERE NAME LIKE "2014_01_02" LIMIT 10;
    ```

1. 展示指定 Resource ，指定 NAME 为 "20140102" 并按 KEY 降序排序
    
    ```sql
    SHOW RESOURCES WHERE NAME = "20140102" ORDER BY `KEY` DESC;
    ```

### Keywords

    SHOW, RESOURCES

### Best Practice

