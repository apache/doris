---
{
    "title": "ADMIN SET CONFIG",
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

# ADMIN SET CONFIG
## description

    该语句用于设置集群的配置项（当前仅支持设置FE的配置项）。
    可设置的配置项，可以通过 ADMIN SHOW FRONTEND CONFIG; 命令查看。

    语法：

        ADMIN SET FRONTEND CONFIG ("key" = "value");

## example

    1. 设置 'disable_balance' 为 true

        ADMIN SET FRONTEND CONFIG ("disable_balance" = "true");

## keyword
    ADMIN,SET,CONFIG
