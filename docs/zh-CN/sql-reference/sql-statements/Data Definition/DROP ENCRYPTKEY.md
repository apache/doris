---
{
    "title": "DROP ENCRYPTKEY",
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

# DROP ENCRYPTKEY

## Description

### Syntax

```
DROP ENCRYPTKEY key_name
```

### Parameters

> `key_name`: 要删除密钥的名字, 可以包含数据库的名字。比如：`db1.my_key`。

删除一个自定义密钥。密钥的名字完全一致才能够被删除。

执行此命令需要用户拥有 `ADMIN` 权限。

## example

1. 删除掉一个密钥

```
DROP ENCRYPTKEY my_key;
```

## keyword

    DROP,ENCRYPTKEY
