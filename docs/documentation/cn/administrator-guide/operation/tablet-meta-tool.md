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

# Tablet 元数据管理工具 

## 背景

在最新版本的代码中，我们在 BE 端引入了 RocksDB，用于存储 tablet 的元信息，以解决之前通过 header 文件的方式存储元信息，带来的各种功能和性能方面的问题。当前每一个数据目录（root\_path），都会有一个对应的 RocksDB 实例，其中以 key-value 的方式，存放对应 root\_path 上的所有 tablet 的元数据。

为了方便进行这些元数据的维护，我们提供了在线的 http 接口方式和离线的 meta\_tool 工具以完成相关的管理操作。

其中 http 接口仅用于在线的查看 tablet 的元数据，可以在 BE 进程运行的状态下使用。

而 meta\_tool 工具则仅用于离线的各类元数据管理操作，必须先停止BE进程后，才可使用。

meta\_tool 工具存放在 BE 的 lib/ 目录下。

## 操作

### 查看 Tablet Meta

查看 Tablet Meta 信息可以分为在线方法和离线方法

#### 在线

访问 BE 的 http 接口，获取对应的 Tablet Meta 信息：

api：

`http://{host}:{port}/api/meta/header/{tablet_id}/{schema_hash}`


> host: BE 的 hostname
> 
> port: BE 的 http 端口
> 
> tablet_id: tablet id
> 
> schema_hash: tablet 的 schema hash

举例：
    
`http://be_host:8040/api/meta/header/14156/2458238340`

最终查询成功的话，会将 Tablet Meta 以 json 形式返回。

#### 离线

基于 meta\_tool 工具获取某个盘上的 Tablet Meta。

命令：

```
./lib/meta_tool --root_path=/path/to/root_path --operation=get_meta --tablet_id=xxx --schema_hash=xxx
```

> root_path: 在 be.conf 中配置的对应的 root_path 路径。

结果也是按照 json 的格式展现 Tablet Meta。

### 加载 header

加载 header 的功能是为了完成实现 tablet 人工迁移而提供的。该功能是基于 json 格式的 Tablet Meta 实现的，所以如果涉及 shard 字段、version 信息的更改，可以直接在 Tablet Meta 的 json 内容中更改。然后使用以下的命令进行加载。

命令：

```
./lib/meta_tool --operation=load_meta --root_path=/path/to/root_path --json_header_path=path
```

### 删除 header

为了实现从某个 be 的某个盘中删除某个 tablet 的功能。

命令:

```
./lib/meta_tool --operation=delete_meta --root_path=/path/to/root_path --tablet_id=xxx --schema_hash=xxx
```

### 展示 pb 格式的 TabletMeta

这个命令是为了查看旧的基于文件的管理的PB格式的 Tablet Meta，以 json 的格式展示 Tablet Meta。

命令：

```
./lib/meta_tool --operation=show_meta --root_path=/path/to/root_path --pb_header_path=path
```


