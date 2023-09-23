---

{

"title": "远程 UDAF",

"language": "zh-CN"

}

---

<!--

Licensed to the Apache Software Foundation (ASF) under one

or more contributor license agreements. See the NOTICE file

distributed with this work for additional information

regarding copyright ownership. The ASF licenses this file

to you under the Apache License, Version 2.0 (the

"License"); you may not use this file except in compliance

with the License. You may obtain a copy of the License at

  

http://www.apache.org/licenses/LICENSE-2.0

  

Unless required by applicable law or agreed to in writing,

software distributed under the License is distributed on an

"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY

KIND, either express or implied. See the License for the

specific language governing permissions and limitations

under the License.

-->

  

# 远程UDAF

  

Remote UDAF Service 支持通过 RPC 的方式访问用户提供的 UDAF Service，以实现用户自定义函数的执行。相比于 Native 的 UDAF 实现，Remote UDAF Service 有如下优势和限制：

1. 优势

* 跨语言：可以用 Protobuf 支持的各类语言编写 UDAF Service。

* 安全：UDAF 执行失败或崩溃，仅会影响 UDAF Service 自身，而不会导致 Doris 进程崩溃。

* 灵活：UDAF Service 中可以调用任意其他服务或程序库类，以满足更多样的业务需求。

  

2. 使用限制

* 性能：相比于 Native UDAF，UDAF Service 会带来额外的网络开销，因此性能会远低于 Native UDAF。同时，UDAF Service 自身的实现也会影响函数的执行效率，用户需要自行处理高并发、线程安全等问题。

* 单行模式和批处理模式：Doris 原先的基于行存的查询执行框架会对每一行数据执行一次 UDAF RPC 调用，因此执行效率非常差，而在新的向量化执行框架下，会对每一批数据执行一次 UDAF RPC 调用，因此性能有明显提升。实际测试中，基于向量化和批处理方式的 Remote UDAF 性能和基于行存的 Native UDAF 性能相当，可供参考。

  

## 编写 UDAF 函数

  
  

本小节主要介绍如何开发一个 Remote RPC service。在 `samples/doris-demo/UDAF-demo/` 下提供了 Java 版本的示例，可供参考。

  

### 拷贝 proto 文件

  

拷贝 gensrc/proto/function_service.proto 和 gensrc/proto/types.proto 到 Rpc 服务中

  

- function_service.proto

- PFunctionCallRequest

- function_name：函数名称，对应创建函数时指定的symbol

- args：方法传递的参数

- context：查询上下文信息

- PFunctionCallResponse

- result：结果

- status：状态，0代表正常

- PCheckFunctionRequest

- function：函数相关信息

- match_type：匹配类型

- PCheckFunctionResponse

- status：状态，0代表正常

  

### 生成接口

  

通过 protoc 生成代码，具体参数通过 protoc -h 查看

  

### 实现接口

  

共需要实现以下三个方法

- fnCall：用于编写计算逻辑

- checkFn：用于创建 UDAF 时校验，校验函数名/参数/返回值等是否合法

- handShake：用于接口探活

  

## 创建 UDAF

 
目前暂不支持UDTF

  

```sql

CREATE AGGREGATE FUNCTION

name ([,...])

[RETURNS] rettype

PROPERTIES (["key"="value"][,...])

```

说明：

  

1. PROPERTIES中`symbol`表示的是 rpc 调用传递的方法名，这个参数是必须设定的。

2. PROPERTIES中`object_file`表示的 rpc 服务地址，目前支持单个地址和 brpc 兼容格式的集群地址，集群连接方式 参考 [格式说明](https://github.com/apache/incubator-brpc/blob/master/docs/cn/client.md#%E8%BF%9E%E6%8E%A5%E6%9C%8D%E5%8A%A1%E9%9B%86%E7%BE%A4)。

3. PROPERTIES中`type`表示的 UDAF 调用类型，默认为 Native，使用 Rpc UDAF时传 RPC。


示例：
```sql

CREATE AGGREGATE FUNCTION rpc_sum(int) RETURNS int PROPERTIES (

"TYPE"="RPC",

"OBJECT_FILE"="127.0.0.1:9000",

"update_fn"="rpc_sum_update",

"merge_fn"="rpc_sum_merge",

"finalize_fn"="rpc_sum_finalize"
);

//使用
select rpc_sum(pv) from table1;
```

  

## 使用 UDAF

  

用户使用 UDAF 必须拥有对应数据库的 `SELECT` 权限。

  

UDAF 的使用与普通的函数方式一致，唯一的区别在于，内置函数的作用域是全局的，而 UDAF 的作用域是 DB内部。当链接 session 位于数据内部时，直接使用 UDAF 名字会在当前DB内部查找对应的 UDAF。否则用户需要显示的指定 UDAF 的数据库名字，例如 `dbName`.`funcName`。

  

## 删除 UDAF

  

当你不再需要 UDAF 函数时，你可以通过下述命令来删除一个 UDAF 函数, 可以参考 `DROP FUNCTION`。

  

## 示例

在`samples/doris-demo/` 目录中提供和 cpp/java/python 语言的rpc server 实现示例。具体使用方法见每个目录下的`README.md`