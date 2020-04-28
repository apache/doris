---
{
    "title": "文件管理器",
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

# 文件管理器

Doris 中的一些功能需要使用一些用户自定义的文件。比如用于访问外部数据源的公钥、密钥文件、证书文件等等。文件管理器提供这样一个功能，能够让用户预先上传这些文件并保存在 Doris 系统中，然后可以在其他命令中引用或访问。

## 名词解释

* FE：Frontend，Doris 的前端节点。负责元数据管理和请求接入。
* BE：Backend，Doris 的后端节点。负责查询执行和数据存储。
* BDBJE：Oracle Berkeley DB Java Edition。FE 中用于持久化元数据的分布式嵌入式数据库。
* SmallFileMgr：文件管理器。负责创建并维护用户的文件。

## 基本概念

文件是指用户创建并保存在 Doris 中的文件。

一个文件由 `数据库名称（database）`、`分类（catalog）` 和 `文件名（file_name）` 共同定位。同时每个文件也有一个全局唯一的 id（file_id），作为系统内的标识。

文件的创建和删除只能由拥有 `admin` 权限的用户进行操作。一个文件隶属于一个数据库。对某一数据库拥有访问权限（查询、导入、修改等等）的用户都可以使用该数据库下创建的文件。

## 具体操作

文件管理主要有三个命令：`CREATE FILE`，`SHOW FILE` 和 `DROP FILE`，分别为创建、查看和删除文件。这三个命令的具体语法可以通过连接到 Doris 后，执行 `HELP cmd;` 的方式查看帮助。

1. CREATE FILE

    在创建文件的命令中，用户必须提供以下信息：
    
    * file_name：文件名。用户自定义，在一个 catalog 内唯一即可。
    * catalog：文件所属分类。用户自定义，在一个 database 内唯一即可。
        
        > Doris 也有一些特殊的分类名称供特定的命令使用。
        
        > 1. kafka
        
        >   当在例行导入命令中指定数据源为 Kafka，并且需要引用到文件时，Doris 会默认从 catalog 名为 "kafka" 的分类中查找文件。
        
    * url：文件的下载地址。目前仅支持无认证的 http 下载地址。该下载地址仅用于在执行创建文件命令时，从这个地址下载文件。当文件成功创建并保存在 Doris 中后，该地址将不再被使用。
    * md5：可选项。文件的 MD5 值。如果用户提供该值，将在文件下载后进行 MD5 值的校验。校验失败则文件创建失败。

    文件创建成功后，文件相关的信息将持久化在 Doris 中。用户可以通过 `SHOW FILE` 命令查看已经创建成功的文件。
    
2. SHOW FILE

    该命令可以查看已经创建成功的文件。具体操作见：`HELP SHOW FILE;`
    
3. DROP FILE

    该命令可以删除一个已经创建的文件。具体操作见：`HELP DROP FILE;`
    
## 实现细节

### 创建和删除文件

当用户执行 `CREATE FILE` 命令后，FE 会从给定的 URL 下载文件。并将文件的内容以 Base64 编码的形式直接保存在 FE 的内存中。同时会将文件内容以及文件相关的元信息持久化在 BDBJE 中。所有被创建的文件，其元信息和文件内容都会常驻于 FE 的内存中。如果 FE 宕机重启，也会从 BDBJE 中加载元信息和文件内容到内存中。当文件被删除时，会直接从 FE 内存中删除相关信息，同时也从 BDBJE 中删除持久化的信息。
    
### 文件的使用

如果是 FE 端需要使用创建的文件，则 SmallFileMgr 会直接将 FE 内存中的数据保存为本地文件，存储在指定的目录中，并返回本地的文件路径供使用。

如果是 BE 端需要使用创建的文件，BE 会通过 FE 的 http 接口 `/api/get_small_file` 将文件内容下载到 BE 上指定的目录中，供使用。同时，BE 也会在内存中记录当前已经下载过的文件的信息。当 BE 请求一个文件时，会先查看本地文件是否存在并校验。如果校验通过，则直接返回本地文件路径。如果校验失败，则会删除本地文件，重新从 FE 下载。当 BE 重启时，会预先加载本地的文件到内存中。

## 使用限制

因为文件元信息和内容都存储于 FE 的内存中。所以默认仅支持上传大小在 1MB 以内的文件。并且总文件数量限制为 100 个。可以通过下一小节介绍的配置项进行修改。

## 相关配置

1. FE 配置

    * `small_file_dir`：用于存放上传文件的路径，默认为 FE 运行目录的 `small_files/` 目录下。
    * `max_small_file_size_bytes`：单个文件大小限制，单位为字节。默认为 1MB。大于该配置的文件创建将会被拒绝。
    * `max_small_file_number`：一个 Doris 集群支持的总文件数量。默认为 100。当创建的文件数超过这个值后，后续的创建将会被拒绝。

    > 如果需要上传更多文件或提高单个文件的大小限制，可以通过 `ADMIN SET CONFIG` 命令修改 `max_small_file_size_bytes` 和 `max_small_file_number` 参数。但文件数量和大小的增加，会导致 FE 内存使用量的增加。
    
2. BE 配置

    * `small_file_dir`：用于存放从 FE 下载的文件的路径，默认为 BE 运行目录的 `lib/small_files/` 目录下。
