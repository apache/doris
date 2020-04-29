---
{
    "title": "User Define Function",
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

# User Define Function

用户可以通过UDF机制来扩展Doris的能力。通过这篇文档，用户能够创建自己的UDF。

## 编写UDF函数

在使用UDF之前，用户需要先在Doris的UDF框架下，编写自己的UDF函数。在`be/src/udf_samples/udf_sample.h|cpp`文件中是一个简单的UDF Demo。

编写一个UDF函数需要以下几个步骤。

### 编写函数

创建对应的头文件、CPP文件，在CPP文件中实现你需要的逻辑。CPP文件中的实现函数格式与UDF的对应关系。

#### 非可变参数

对于非可变参数的UDF，那么两者之间的对应关系很直接。
比如`INT MyADD(INT, INT)`的UDF就会对应`IntVal AddUdf(FunctionContext* context, const IntVal& arg1, const IntVal& arg2)`。

1. `AddUdf`可以为任意的名字，只要创建UDF的时候指定即可。
2. 实现函数中的第一个参数永远是`FunctionContext*`。实现者可以通过这个结构体获得一些查询相关的内容，以及申请一些需要使用的内存。具体使用的接口可以参考`udf/udf.h`中的定义。
3. 实现函数中从第二个参数开始需要与UDF的参数一一对应，比如`IntVal`对应`INT`类型。这部分的类型都要使用`const`引用。
4. 返回参数与UDF的参数的类型要相对应。

#### 可变参数

对于可变参数，可以参见以下例子，UDF`String md5sum(String, ...)`对应的
实现函数是`StringVal md5sumUdf(FunctionContext* ctx, int num_args, const StringVal* args)`

1. `md5sumUdf`这个也是可以任意改变的，创建的时候指定即可。
2. 第一个参数与非可变参数函数一样，传入的是一个`FunctionContext*`。
3. 可变参数部分由两部分组成，首先会传入一个整数，说明后面还有几个参数。后面传入的是一个可变参数部分的数组。

#### 类型对应关系

|UDF Type|Argument Type|
|----|---------|
|TinyInt|TinyIntVal|
|SmallInt|SmallIntVal|
|Int|IntVal|
|BigInt|BigIntVal|
|LargeInt|LargeIntVal|
|Float|FloatVal|
|Double|DoubleVal|
|Date|DateTimeVal|
|Datetime|DateTimeVal|
|Char|StringVal|
|Varchar|StringVal|
|Decimal|DecimalVal|

## 编译UDF函数

### 编译Doris

在Doris根目录下执行`sh build.sh`就会在`output/udf/`生成对应`headers|libs`

### 编写CMakeLists.txt

基于上一步生成的`headers|libs`，用户可以使用`CMakeLists`等工具引入该依赖；在`CMakeLists`中，可以通过向`CMAKE_CXX_FLAGS`添加`-I|L`分别指定`headers|libs`路径；然后使用`add_library`添加动态库。例如，在`be/src/udf_samples/CMakeLists.txt`中，使用`add_library(udfsample SHARED udf_sample.cpp)` `target_link_libraries`(udfsample -static-libstdc++ -static-libgcc)增加了一个`udfsample`动态库。后面需要写上涉及的所有源文件（不包含头文件）。

### 执行编译

在该目录下创建一个`build`目录并在`build`下执行`cmake ../`生成`Makefile`，并执行`make`就会生成对应动态库。

## 创建UDF函数

通过上述的步骤后，你可以得到一个动态库。你需要将这个动态库放到一个能够通过HTTP协议访问到的位置。然后执行创建UDF函数在Doris系统内部创建一个UDF，你需要拥有AMDIN权限才能够完成这个操作。

```
CREATE [AGGREGATE] FUNCTION 
	name ([argtype][,...])
	[RETURNS] rettype
	PROPERTIES (["key"="value"][,...])
```
说明：

1. PROPERTIES中`symbol`表示的是，执行入口函数的对应symbol，这个参数是必须设定。你可以通过`nm`命令来获得对应的symbol，比如`nm libudfsample.so | grep AddUdf`获得到的`_ZN9doris_udf6AddUdfEPNS_15FunctionContextERKNS_6IntValES4_`就是对应的symbol。
2. PROPERTIES中`object_file`表示的是从哪里能够下载到对应的动态库，这个参数是必须设定的。
3. name: 一个function是要归属于某个DB的，name的形式为`dbName`.`funcName`。当`dbName`没有明确指定的时候，就是使用当前session所在的db作为`dbName`。

具体使用可以参见 `CREATE FUNCTION` 获取更详细信息。

## 使用UDF

用户使用UDF/UDAF必须拥有对应数据库的 `SELECT` 权限。

UDF的使用与普通的函数方式一致，唯一的区别在于，内置函数的作用域是全局的，而UDF的作用域是DB内部。当链接session位于数据内部时，直接使用UDF名字会在当前DB内部查找对应的UDF。否则用户需要显示的指定UDF的数据库名字，例如`dbName`.`funcName`。


## 删除UDF函数

当你不再需要UDF函数时，你可以通过下述命令来删除一个UDF函数, 可以参考 `DROP FUNCTION`。

