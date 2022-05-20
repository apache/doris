---
{
"title": "Java UDF",
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

# Java UDF

Java UDF 为用户提供UDF编写的Java接口，以方便用户使用Java语言进行自定义函数的执行。相比于 Native 的 UDF 实现，Java UDF 有如下优势和限制：
1. 优势
* 兼容性：使用Java UDF可以兼容不同的Doris版本，所以在进行Doris版本升级时，Java UDF不需要进行额外的迁移操作。与此同时，Java UDF同样遵循了和Hive/Spark等引擎同样的编程规范，使得用户可以直接将Hive/Spark的UDF jar包迁移至Doris使用。
* 安全：Java UDF 执行失败或崩溃仅会导致JVM报错，而不会导致 Doris 进程崩溃。
* 灵活：Java UDF 中用户通过把第三方依赖打进用户jar包，而不需要额外处理引入的三方库。

2. 使用限制
* 性能：相比于 Native UDF，Java UDF会带来额外的JNI开销，不过通过批式执行的方式，我们已经尽可能的将JNI开销降到最低。
* 向量化引擎：Java UDF当前只支持向量化引擎。

## 编写 UDF 函数

本小节主要介绍如何开发一个 Java UDF。在 `samples/doris-demo/java-udf-demo/` 下提供了示例，可供参考，查看点击[这里](https://github.com/apache/incubator-doris/tree/master/samples/doris-demo/java-udf-demo)

使用Java代码编写UDF，UDF的主入口必须为 `evaluate` 函数。这一点与Hive等其他引擎保持一致。在本示例中，我们编写了 `AddOne` UDF来完成对整型输入进行加一的操作。
值得一提的是，本例不只是Doris支持的Java UDF，同时还是Hive支持的UDF，也就是说，对于用户来讲，Hive UDF是可以直接迁移至Doris的。

#### 类型对应关系

|UDF Type|Argument Type|
|----|---------|
|TinyInt|TinyIntVal|
|SmallInt|Short|
|Int|Integer|
|BigInt|Long|
|LargeInt|BigInteger|
|Float|Float|
|Double|Double|
|Date|LocalDate|
|Datetime|LocalDateTime|
|Char|String|
|Varchar|String|
|Decimal|BigDecimal|


## 创建 UDF

目前暂不支持 UDAF 和 UDTF

```sql
CREATE FUNCTION 
name ([,...])
[RETURNS] rettype
PROPERTIES (["key"="value"][,...])	
```
说明：

1. PROPERTIES中`symbol`表示的是包含UDF类的类名，这个参数是必须设定的。
2. PROPERTIES中`file`表示的包含用户UDF的jar包，这个参数是必须设定的。
3. PROPERTIES中`type`表示的 UDF 调用类型，默认为 Native，使用 Java UDF时传 JAVA_UDF。
4. name: 一个function是要归属于某个DB的，name的形式为`dbName`.`funcName`。当`dbName`没有明确指定的时候，就是使用当前session所在的db作为`dbName`。

示例：
```sql
CREATE FUNCTION java_udf_add_one(int) RETURNS int PROPERTIES (
    "file"="file:///path/to/java-udf-demo-jar-with-dependencies.jar",
    "symbol"="org.apache.doris.udf.AddOne",
    "type"="JAVA_UDF"
);
```

## 使用 UDF

用户使用 UDF 必须拥有对应数据库的 `SELECT` 权限。

UDF 的使用与普通的函数方式一致，唯一的区别在于，内置函数的作用域是全局的，而 UDF 的作用域是 DB内部。当链接 session 位于数据内部时，直接使用 UDF 名字会在当前DB内部查找对应的 UDF。否则用户需要显示的指定 UDF 的数据库名字，例如 `dbName`.`funcName`。

## 删除 UDF

当你不再需要 UDF 函数时，你可以通过下述命令来删除一个 UDF 函数, 可以参考 `DROP FUNCTION`。

## 示例
在`samples/doris-demo/java-udf-demo/` 目录中提供了具体示例。具体使用方法见每个目录下的`README.md`，查看点击[这里](https://github.com/apache/incubator-doris/tree/master/samples/doris-demo/java-udf-demo)

## 暂不支持的场景
当前Java UDF仍然处在持续的开发过程中，所以部分功能**尚不完善**。包括：
1. 不支持复杂数据类型（HLL，Bitmap）
2. 尚未统一JVM和Doris的内存管理以及统计信息

