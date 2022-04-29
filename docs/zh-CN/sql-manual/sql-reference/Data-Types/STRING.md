---
{
    "title": "STRING",
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

## STRING
### description
    STRING
    变长字符串，最大支持2147483643 字节（2GB-4）。String类型的长度还受 be 配置  `string_type_soft_limit`, 实际能存储的最大长度 取两者最小值，String类型只能用在value 列，不能用在 key 列和分区 分桶列
    
    注意：变长字符串是以UTF-8编码存储的，因此通常英文字符占1个字节，中文字符占3个字节。

### keywords

    STRING
