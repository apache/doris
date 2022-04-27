---
{
    "title": "DROP-FUNCTION",
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

## DROP-FUNCTION

### Name

DROP FUNCTION

### Description

删除一个自定义函数。函数的名字、参数类型完全一致才能够被删除

语法：

```sql
DROP FUNCTION function_name
    (arg_type [, ...])
```

参数说明：

- `function_name`: 要删除函数的名字
- `arg_type`: 要删除函数的参数列表

### Example

1. 删除掉一个函数

   ```sql
   DROP FUNCTION my_add(INT, INT)
   ```

### Keywords

    DROP, FUNCTION

### Best Practice

