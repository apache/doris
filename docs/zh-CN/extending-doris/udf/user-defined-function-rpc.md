---
{
    "title": "User Defined Function Rpc",
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

# User Defined Function Rpc

可以通过 Rpc 的方式调用函数逻辑，通过 protobuf 进行数据传输，支持 Java/C++/Python/Ruby/Go/PHP/JavaScript 等多种语言

## 编写 UDF 函数

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
- checkFn：用于创建 UDF 时校验，校验函数名/参数/返回值等是否合法
- handShake：用于接口探活

## 创建 UDF

目前暂不支持 UDAF 和 UDTF

```
CREATE FUNCTION 
	name ([,...])
	[RETURNS] rettype
	PROPERTIES (["key"="value"][,...])
	
```
说明：

1. PROPERTIES中`symbol`表示的是 rpc 调用传递的方法名，这个参数是必须设定的。
2. PROPERTIES中`object_file`表示的 rpc 服务地址，目前支持单个地址和 brpc 兼容格式的集群地址，集群连接方式 参考 [格式说明](https://github.com/apache/incubator-brpc/blob/master/docs/cn/client.md#%E8%BF%9E%E6%8E%A5%E6%9C%8D%E5%8A%A1%E9%9B%86%E7%BE%A4)。
3. PROPERTIES中`type`表示的 UDF 调用类型，默认为 Native，使用 Rpc UDF时传 RPC。
4. name: 一个function是要归属于某个DB的，name的形式为`dbName`.`funcName`。当`dbName`没有明确指定的时候，就是使用当前session所在的db作为`dbName`。

示例：
```
CREATE FUNCTION rpc_add(INT, INT) RETURNS INT PROPERTIES (
  "SYMBOL"="add_int",
  "OBJECT_FILE"="127.0.0.1:9999",
  "TYPE"="RPC"
);
```

## 使用 UDF

用户使用 UDF 必须拥有对应数据库的 `SELECT` 权限。

UDF 的使用与普通的函数方式一致，唯一的区别在于，内置函数的作用域是全局的，而 UDF 的作用域是 DB内部。当链接 session 位于数据内部时，直接使用 UDF 名字会在当前DB内部查找对应的 UDF。否则用户需要显示的指定 UDF 的数据库名字，例如 `dbName`.`funcName`。

## 删除 UDF

当你不再需要 UDF 函数时，你可以通过下述命令来删除一个 UDF 函数, 可以参考 `DROP FUNCTION`。

