---
{
    "title": "UNSET-VARIABLE",
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

<version since="dev">

## UNSET-VARIABLE

</version>

### Name

UNSET VARIABLE

### Description

该语句主要是用来恢复 Doris 系统变量为默认值，可以是全局也可以是会话级别。

语法：

```sql
UNSET [SESSION|GLOBAL] VARIABLE (variable_name | ALL)
```

说明：

1. (variable_name | ALL) ：必须指定变量名或使用 ALL , ALL 会恢复所有变量的值。

> 注意：
>
> 1. 只有 ADMIN 用户可以全局得恢复变量的值。
> 2. 使用 `GLOBAL` 恢复变量值时仅在执行命令的当前会话和之后打开的会话中生效，不会恢复当前已有的其它会话中的值。


### Example

1. 恢复时区为默认值东八区

   ```
   UNSET VARIABLE time_zone;
   ```

2. 恢复全局的执行内存大小

   ```
   UNSET GLOBAL VARIABLE exec_mem_limit;
   ```

3. 从全局范围恢复所有变量的值

   ```
   UNSET GLOBAL VARIABLE ALL;
   ```

### Keywords

    UNSET, VARIABLE

### Best Practice

