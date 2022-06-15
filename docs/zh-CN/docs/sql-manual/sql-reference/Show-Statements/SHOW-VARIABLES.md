---
{
    "title": "SHOW-VARIABLES",
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

## SHOW-VARIABLES

### Name

SHOW VARIABLES

### Description

改语句是用来显示Doris系统变量，可以通过条件查询

语法：

```sql
SHOW [GLOBAL | SESSION] VARIABLES
    [LIKE 'pattern' | WHERE expr]
```

说明：

- show variables主要是用来查看系统变量的值.
- 执行SHOW VARIABLES命令不需要任何权限,只要求能够连接到服务器就可以.
- 使用like语句表示用variable_name进行匹配.
- %百分号通配符可以用在匹配模式中的任何位置

### Example

1. 这里默认的就是对Variable_name进行匹配,这里是准确匹配

   ```sql
   show variables like 'max_connections'; 
   ```
   
2. 通过百分号(%)这个通配符进行匹配,可以匹配多项

   ```sql
   show variables like '%connec%';
   ```

3. 使用 Where 子句进行匹配查询

   ```sql
   show variables where variable_name = 'version';
   ```

### Keywords

    SHOW, VARIABLES

### Best Practice

