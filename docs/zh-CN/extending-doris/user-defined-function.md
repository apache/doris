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

UDF 主要适用于，用户需要的分析能力 Doris 并不具备的场景。用户可以自行根据自己的需求，实现自定义的函数，并且通过 UDF 的方式注册到 Doris 中，来扩展 Doris 的能力，并解决用户分析需求。

UDF 能满足的分析需求分为两种：UDF 和 UDAF。本文中的 UDF 指的是二者的统称。

1. UDF: 用户自定义函数，这种函数会对单行进行操作，并且输出单行结果。当用户在查询时使用 UDF ，每行数据最终都会出现在结果集中。典型的 UDF 比如字符串操作 concat() 等。
2. UDAF: 用户自定义的聚合函数，这种函数对多行进行操作，并且输出单行结果。当用户在查询时使用 UDAF，分组后的每组数据最后会计算出一个值并展结果集中。典型的 UDAF 比如集合操作 sum() 等。一般来说 UDAF 都会结合 group by 一起使用。 

这篇文档主要讲述了，如何编写自定义的 UDF 函数，以及如何在 Doris 中使用它。

如果用户使用 UDF 功能并扩展了 Doris 的函数分析，并且希望将自己实现的 UDF 函数贡献回 Doris 社区给其他用户使用，这时候请看文档 [Contribute UDF to Doris](http://doris.apache.org/master/zh-CN/extending-doris/contribute_udf.html)。

## 编写UDF函数

在使用UDF之前，用户需要先在Doris的UDF框架下，编写自己的UDF函数。在`custom_udf/src/udf_samples/udf_sample.h|cpp`文件中是一个简单的UDF Demo。

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

### 编写完成后的目录结构

这里以 udf_sample 为例, 在 src dir 下面创建一个 `udf_samples` 目录用于存放 source code.

```

├── be
├── custom_udf
│   ├── CMakeLists.txt
│   ├── build_custom_udf.sh
│   └── src
│       └── udf_samples
│           ├── CMakeLists.txt
│           ├── uda_sample.cpp
│           ├── uda_sample.h
│           ├── udf_sample.cpp
│           └── udf_sample.h

```

## 编译 UDF 函数

由于用户自己实现的 function 中依赖了 Doris 的 udf , 所以在编译 UDF 函数的时候首先对 Doris 进行编译。然后再编译用户自己实现的 UDF 即可。

### 编译Doris

在Doris根目录下执行`sh build.sh`就会在`output/udf/`生成对应`headers|libs`

```
├── output
│   └── udf
│       ├── include
│       │   ├── uda_test_harness.h
│       │   └── udf.h
│       └── lib
│           └── libDorisUdf.a

```

### 编写自定义 UDF 的 CMakeLists.txt

1. 在 `custom_udf/CMakeLists.txt` 下增加对自定义 UDF 的编译。以 udf_samples 为例

    ```
    ├── be
    ├── custom_udf
    │   ├── CMakeLists.txt
    │   └── src

    
    custom_udf/CMakeLists.txt
    ...
    add_subdirectory(${SRC_DIR}/udf_samples)
    ...

    ```

2. 在自定义 UDF 中增加依赖。以 udf_samples 为例，

    由于 udf_samples 中的代码都没有依赖任何其他库，则不需要声明。
    
    如果代码中依赖了比如 Doris UDF 中对 `StringVal` 的函数，则需要声明依赖了 udf。修改 `udf_samples/CMakeFiles.txt`:

    ```
    ├── be
    ├── custom_udf
    │   ├── CMakeLists.txt
    │   └── src
    │       └── udf_samples
    │           ├── CMakeLists.txt
    
    custom_udf/src/udf_samples/CMakeFiles.txt
    ...
    target_link_libraries(udfsample
        udf
        -static-libstdc++
        -static-libgcc
    )    
    ...

    ```

### 执行编译

运行 custom_udf 下的 `build_custom_udf.sh`

```
├── be
├── custom_udf
│   ├── build_custom_udf.sh

build_custom_udf.sh --udf --clean

```

这个编译脚本如果默认不传入任何参数，则直接编译并且不 clean。如果需要 clean 后再编译则需要加上参数 `--udf --clean`

### 编译结果

编译完成后的动态链接库被放在了 `output/custom_udf/` 下，以 udf_samples 为例，目录结构如下：

```

├── output
│   ├── be
│   ├── custom_udf
│   │   └── lib
│   │       └── udf_samples
│   │           ├── libudasample.so
│   │           └── libudfsample.so

```

## 创建UDF函数

通过上述的步骤后，你可以得到一个动态库。你需要将这个动态库放到一个能够通过 HTTP 协议访问到的位置。然后执行创建 UDF 函数在 Doris 系统内部创建一个 UDF，你需要拥有AMDIN权限才能够完成这个操作。

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

