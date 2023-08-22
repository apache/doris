---
{
    "title": "DIGITAL_MASKING",
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

## DIGITAL_MASKING

### description

#### Syntax

```
digital_masking(digital_number)
```

别名函数，原始函数为 `concat(left(id,3),'****',right(id,4))`。

将输入的 `digital_number` 进行脱敏处理，返回遮盖脱敏后的结果。`digital_number` 为 `BIGINT` 数据类型。

### example

1. 将手机号码进行脱敏处理

    ```sql
    mysql> select digital_masking(13812345678);
    +------------------------------+
    | digital_masking(13812345678) |
    +------------------------------+
    | 138****5678                  |
    +------------------------------+
    ```

### keywords

DIGITAL_MASKING
