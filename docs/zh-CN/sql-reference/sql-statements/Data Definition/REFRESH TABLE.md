---
{
    "title": "REFRESH TABLE",
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

# REFRESH TABLE

## Description

    该语句用于同步远端 Iceberg 表，会将 Doris 当前的外表删除重建。
    语法：
        REFRESH TABLE tbl_name;

    说明：
        1) 仅针对 Doris 中挂载的 Iceberg 表有效。
        
## Example

    1. 刷新表 iceberg_tbl
        REFRESH TABLE iceberg_tbl;
        
## keyword

    REFRESH,TABLE
        
