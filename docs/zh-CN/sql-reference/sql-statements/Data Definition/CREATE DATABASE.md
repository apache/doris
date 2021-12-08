---
{
    "title": "CREATE DATABASE",
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

# CREATE DATABASE

## Description

    该语句用于新建数据库（database）
    语法：
        CREATE DATABASE [IF NOT EXISTS] db_name
        [ENGINE = [builtin|iceberg]]
        [PROPERTIES ("key"="value", ...)];

1. ENGINE 类型
    默认为 builtin，可缺省。可选 iceberg
    1）如果是 iceberg，则需要在 properties 中提供以下信息：
    ```
        PROPERTIES (
            "database" = "iceberg_db_name",
            "hive.metastore.uris" = "thrift://127.0.0.1:9083",
            "catalog.type" = "HIVE_CATALOG"
            )

    ```
    其中 `database` 是 Iceberg 对应的库名；  
    `hive.metastore.uris` 是 hive metastore 服务地址。  
    `catalog.type` 默认为 `HIVE_CATALOG`。当前仅支持 `HIVE_CATALOG`，后续会支持更多 Iceberg catalog 类型。

## example
    1. 新建数据库 db_test
        ```
        CREATE DATABASE db_test;
        ```
        
    2. 新建 Iceberg 数据库 iceberg_test
        ```
        CREATE DATABASE `iceberg_test`
        ENGINE = ICEBERG
        PROPERTIES (
        "database" = "doris",
        "hive.metastore.uris" = "thrift://127.0.0.1:9083",
        "catalog.type" = "HIVE_CATALOG"
        );
        ```

## keyword
    CREATE,DATABASE
    
