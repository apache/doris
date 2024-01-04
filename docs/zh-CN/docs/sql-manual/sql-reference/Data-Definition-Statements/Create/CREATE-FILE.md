---
{
    "title": "CREATE-FILE",
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

## CREATE-FILE

### Name

CREATE FILE

### Description

该语句用于创建并上传一个文件到 Doris 集群。
该功能通常用于管理一些其他命令中需要使用到的文件，如证书、公钥私钥等等。

该命令只用 `admin` 权限用户可以执行。
某个文件都归属与某一个的 database。对 database 拥有访问权限的用户都可以使用该文件。

单个文件大小限制为 1MB。
一个 Doris 集群最多上传 100 个文件。

语法：

```sql
CREATE FILE "file_name" [IN database]
PROPERTIES("key"="value", ...)
```

说明：

- file_name:  自定义文件名。
- database: 文件归属于某一个 db，如果没有指定，则使用当前 session 的 db。
- properties 支持以下参数:
    - url：必须。指定一个文件的下载路径。当前仅支持无认证的 http 下载路径。命令执行成功后，文件将被保存在 doris 中，该 url 将不再需要。
    - catalog：必须。对文件的分类名，可以自定义。但在某些命令中，会查找指定 catalog 中的文件。比如例行导入中的，数据源为 kafka 时，会查找 catalog 名为 kafka 下的文件。
    - md5: 可选。文件的 md5。如果指定，会在下载文件后进行校验。

### Example

1. 创建文件 ca.pem ，分类为 kafka

   ```sql
   CREATE FILE "ca.pem"
   PROPERTIES
   (
       "url" = "https://test.bj.bcebos.com/kafka-key/ca.pem",
       "catalog" = "kafka"
   );
   ```

2. 创建文件 client.key，分类为 my_catalog

   ```sql
   CREATE FILE "client.key"
   IN my_database
   PROPERTIES
   (
       "url" = "https://test.bj.bcebos.com/kafka-key/client.key",
       "catalog" = "my_catalog",
       "md5" = "b5bb901bf10f99205b39a46ac3557dd9"
   );
   ```

### Keywords

```text
CREATE, FILE
```

### Best Practice

1. 该命令只有 amdin 权限用户可以执行。某个文件都归属与某一个的 database。对 database 拥有访问权限的用户都可以使用该文件。

2. 文件大小和数量限制。

   这个功能主要用于管理一些证书等小文件。因此单个文件大小限制为 1MB。一个 Doris 集群最多上传 100 个文件。

