---
{
"title": "WASM UDF",
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

# WASM UDF

<version since="1.2.0">

WASM (WebAssembly) UDF 提供了一种在 Doris 中使用 WebAssembly 编写自定义函数的方式。与 Java UDF 相比，WASM UDF 有一些独特的优势和限制：

1. 优势：

* 跨语言兼容性：WebAssembly 是一种跨语言的二进制指令集，允许使用多种编程语言编译成 UDF。这样，用户可以选择他们熟悉的语言，而不受特定语言的限制。
* 性能：WebAssembly 的执行速度通常比解释型语言更快。通过将 UDF 编译为 WebAssembly，用户可以获得更高的性能，特别是在涉及计算密集型任务时。
* 轻量级：WebAssembly 的二进制格式相对较小，因此传输和加载的开销较低，使得在分布式计算环境中更为轻便。

2. 使用限制：

* 生态系统：WebAssembly 的生态系统相对较新，缺少一些成熟的库和工具。用户可能需要自己编写一些工具来满足自己的需求。
* 调试难度：WebAssembly 的调试相对复杂，因为它是一个低级的二进制格式。与高级语言相比，用户可能需要更多的工具和技能来调试和优化其 UDF。
* 安全性：虽然 WebAssembly 被设计为在沙盒中运行，但用户仍需谨慎处理潜在的安全风险。在加载 UDF 时需要确保来自可信源。

</version>

## 创建 UDF

```sql
CREATE FUNCTION 
name ([,...])
[RETURNS] rettype
PROPERTIES (["key"="value"][,...])	
```
说明：

1. PROPERTIES中`symbol`表示的是包含 UDF 类的类名，这个参数是必须设定的。
2. PROPERTIES中`file`表示的包含用户 UDF 的 wat 文件，这个参数是必须设定的。
3. PROPERTIES中`type`表示的 UDF 调用类型，默认为 Native，使用 WASM UDF 时传 WASM_UDF。
4. PROPERTIES中`always_nullable`表示的 UDF 返回结果中是否有可能出现 NULL 值，是可选参数，默认值为 true。
5. name: 一个 function 是要归属于某个 DB 的，name 的形式为 `dbName`.`funcName`。当 `dbName` 没有明确指定的时候，就是使用当前 session 所在的 db 作为 `dbName`。

示例：
```sql
CREATE FUNCTION wasm_udf_add_one(int) RETURNS int PROPERTIES (
    "file"="file:///path/to/wasm-udf-demo.wat,
    "symbol"="add",
    "always_nullable"="true",
    "type"="WASM_UDF"
);
```
* "file"="http://IP:port/udf.wat", 当在多机环境时，也可以使用 http 的方式下载 wat 文件
* "always_nullable" 可选属性, 如果在计算中对出现的 NULL 值有特殊处理，确定结果中不会返回 NULL，可以设为 false，这样在整个查询计算过程中性能可能更好些。
* 如果你是**本地路径**方式，这里数据库驱动依赖的 wat 文件，**FE、BE节点都要放置**

## 编写 UDF 函数

目前还暂不支持 UDTF

```sql
CREATE FUNCTION wasm_udf_add_one(int) RETURNS int PROPERTIES (
    "file"="file:///path/to/wasm-udf-demo.wat,
    "symbol"="add",
    "always_nullable"="true",
    "type"="WASM_UDF"
);
```

```wat
(module
    (func $add (param i32) (param i32) (result i32)
	(local.get 0)
	(local.get 1)
	(i32.add)
    )
  (export "add" (func $add))
)
```


## 使用 UDF

用户使用 UDF 必须拥有对应数据库的 `SELECT` 权限。

UDF 的使用与普通的函数方式一致，唯一的区别在于，内置函数的作用域是全局的，而 UDF 的作用域是 DB 内部。当链接 session 位于数据内部时，直接使用 UDF 名字会在当前DB内部查找对应的 UDF。否则用户需要显示的指定 UDF 的数据库名字，例如 `dbName`.`funcName`。

## 删除 UDF

当你不再需要 UDF 函数时，你可以通过下述命令来删除一个 UDF 函数, 可以参考 `DROP FUNCTION`。

## 使用须知
1. 不支持除 INT，FLOAT 之外的数据类型。


