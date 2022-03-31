---
{
    "title": "SHOW-PROPERTY",
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

## SHOW-PROPERTY

### Description

该语句用于查看用户的属性

```
SHOW PROPERTY [FOR user] [LIKE key];
```

* `user`

    查看指定用户的属性。如不指定，查看当前用户的。
    
* `LIKE`

    可以通过属性名模糊匹配。
    
返回结果说明：

```sql
mysql> show property like '%connection%';
+----------------------+-------+
| Key                  | Value |
+----------------------+-------+
| max_user_connections | 100   |
+----------------------+-------+
1 row in set (0.01 sec)
```

* `Key`

    属性名。
    
* `Value`

    属性值。

### Example

1. 查看 jack 用户的属性
    
    ```sql
    SHOW PROPERTY FOR 'jack';
    ```

2. 查看 jack 用户连接数限制属性

    ```sql
    SHOW PROPERTY FOR 'jack' LIKE '%connection%';
    ```

### Keywords

    SHOW, PROPERTY

### Best Practice
