---
{
    "title": "ARRAY",
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

# ARRAY
## description
    ARRAY<INT> / ARRAY<VARCHAR<M>>
    数组类型，ARRAY<INT> 表示int类型的数组
    ARRAY<VARCHAR<M>> 表示变长字符串类型的数组，M 的范围是1-65535
    目前, 对于 array 类型仅支持 insert 导入、select 操作

## keyword

    ARRAY
