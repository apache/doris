---
{
    "title": "原生UDF",
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

# UDF

<version deprecated="1.2" comment="请使用 JAVA UDF">

UDF 主要适用于，用户需要的分析能力 Doris 并不具备的场景。用户可以自行根据自己的需求，实现自定义的函数，并且通过 UDF 框架注册到 Doris 中，来扩展 Doris 的能力，并解决用户分析需求。

UDF 能满足的分析需求分为两种：UDF 和 UDAF。本文中的 UDF 指的是二者的统称。

1. UDF: 用户自定义函数，这种函数会对单行进行操作，并且输出单行结果。当用户在查询时使用 UDF ，每行数据最终都会出现在结果集中。典型的 UDF 比如字符串操作 concat() 等。
2. UDAF: 用户自定义的聚合函数，这种函数对多行进行操作，并且输出单行结果。当用户在查询时使用 UDAF，分组后的每组数据最后会计算出一个值并展结果集中。典型的 UDAF 比如集合操作 sum() 等。一般来说 UDAF 都会结合 group by 一起使用。

这篇文档主要讲述了，如何编写自定义的 UDF 函数，以及如何在 Doris 中使用它。

如果用户使用 UDF 功能并扩展了 Doris 的函数分析，并且希望将自己实现的 UDF 函数贡献回 Doris 社区给其他用户使用，这时候请看文档 [Contribute UDF](./contribute-udf.md)。

</version>

## 编写 UDF 函数

在使用UDF之前，用户需要先在 Doris 的 UDF 框架下，编写自己的UDF函数。在`contrib/udf/src/udf_samples/udf_sample.h|cpp`文件中是一个简单的 UDF Demo。

编写一个 UDF 函数需要以下几个步骤。

### 编写函数

创建对应的头文件、CPP文件，在CPP文件中实现你需要的逻辑。CPP文件中的实现函数格式与UDF的对应关系。

用户可以把自己的 source code 统一放在一个文件夹下。这里以 udf_sample 为例，目录结构如下：

```
└── udf_samples
  ├── uda_sample.cpp
  ├── uda_sample.h
  ├── udf_sample.cpp
  └── udf_sample.h
```

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


## 编译 UDF 函数

由于 UDF 实现中依赖了 Doris 的 UDF 框架 , 所以在编译 UDF 函数的时候首先要对 Doris 进行编译，也就是对 UDF 框架进行编译。

编译完成后会生成，UDF 框架的静态库文件。之后引入 UDF 框架依赖，并编译 UDF 即可。

### 编译Doris

在 Doris 根目录下执行 `sh build.sh` 就会在 `output/udf/` 生成 UDF 框架的静态库文件 `headers|libs`

```
├── output
│   └── udf
│       ├── include
│       │   ├── uda_test_harness.h
│       │   └── udf.h
│       └── lib
│           └── libDorisUdf.a

```

### 编写 UDF 编译文件

1. 准备 thirdparty

   `thirdparty` 文件夹主要用于存放用户 UDF 函数依赖的第三方库，包括头文件及静态库。其中必须包含依赖的 Doris UDF 框架中 `udf.h` 和 `libDorisUdf.a` 这两个文件。

   这里以 `udf_sample` 为例, 在 用户自己 `udf_samples` 目录用于存放 source code。在同级目录下再创建一个 `thirdparty` 文件夹用于存放静态库。目录结构如下：

    ```
    ├── thirdparty
    │ │── include
    │ │ └── udf.h
    │ └── lib
    │   └── libDorisUdf.a
    └── udf_samples

    ```

   `udf.h` 是 UDF 框架头文件。存放路径为 `doris/output/udf/include/udf.h`。 用户需要将 Doris 编译产出中的这个头文件拷贝到自己的 `thirdparty` 的 include 文件夹下。

   `libDorisUdf.a`  是 UDF 框架的静态库。Doris 编译完成后该文件存放在 `doris/output/udf/lib/libDorisUdf.a`。用户需要将该文件拷贝到自己的 `thirdparty` 的 lib 文件夹下。

   *注意：UDF 框架的静态库只有完成 Doris 编译后才会生成。

2. 准备编译 UDF 的 CMakeFiles.txt

   CMakeFiles.txt 用于声明 UDF 函数如何进行编译。存放在源码文件夹下，与用户代码平级。这里以 `udf_samples` 为例目录结构如下:

    ```
    ├── thirdparty
    └── udf_samples
      ├── CMakeLists.txt
      ├── uda_sample.cpp
      ├── uda_sample.h
      ├── udf_sample.cpp
      └── udf_sample.h
    ```

    + 需要显示声明引用 `libDorisUdf.a`
    + 声明 `udf.h` 头文件位置


    以 udf_sample 为例    
    
    ```
    # Include udf
    include_directories(../thirdparty/include)    

    # Set all libraries
    add_library(udf STATIC IMPORTED)
    set_target_properties(udf PROPERTIES IMPORTED_LOCATION ../thirdparty/lib/libDorisUdf.a)

    # where to put generated libraries
    set(LIBRARY_OUTPUT_PATH "src/udf_samples")

    # where to put generated binaries
    set(EXECUTABLE_OUTPUT_PATH "src/udf_samples")

    add_library(udfsample SHARED udf_sample.cpp)
        target_link_libraries(udfsample
        udf
        -static-libstdc++
        -static-libgcc
    )

    add_library(udasample SHARED uda_sample.cpp)
        target_link_libraries(udasample
        udf
        -static-libstdc++
        -static-libgcc
    )
    ```

    如果用户的 UDF 函数还依赖了其他的三方库，则需要声明 include，lib，并在 `add_library` 中增加依赖。

所有文件准备齐后完整的目录结构如下：

```
    ├── thirdparty
    │ │── include
    │ │ └── udf.h
    │ └── lib
    │   └── libDorisUdf.a
    └── udf_samples
      ├── CMakeLists.txt
      ├── uda_sample.cpp
      ├── uda_sample.h
      ├── udf_sample.cpp
      └── udf_sample.h
```

准备好上述文件就可以直接编译 UDF 了

### 执行编译

在 udf_samples 文件夹下创建一个 build 文件夹，用于存放编译产出。

在 build 文件夹下运行命令 `cmake ../` 生成Makefile，并执行 make 就会生成对应动态库。

```
├── thirdparty
├── udf_samples
  └── build
```

### 编译结果

编译完成后的 UDF 动态链接库就生成成功了。在 `build/src/` 下，以 udf_samples 为例，目录结构如下：

```

├── thirdparty
├── udf_samples
  └── build
    └── src
      └── udf_samples
        ├── libudasample.so
        └── libudfsample.so

```

## 创建 UDF 函数

通过上述的步骤后，你可以得到 UDF 的动态库（也就是编译结果中的 `.so` 文件）。你需要将这个动态库放到一个能够通过 HTTP 协议访问到的位置。

然后登录 Doris 系统，在 mysql-client 中通过 `CREATE FUNCTION` 语法创建 UDF 函数。你需要拥有ADMIN权限才能够完成这个操作。这时 Doris 系统内部就会存在刚才创建好的 UDF。

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

## 使用 UDF

用户使用 UDF 必须拥有对应数据库的 `SELECT` 权限。

UDF 的使用与普通的函数方式一致，唯一的区别在于，内置函数的作用域是全局的，而 UDF 的作用域是 DB内部。当链接 session 位于数据内部时，直接使用 UDF 名字会在当前DB内部查找对应的 UDF。否则用户需要显示的指定 UDF 的数据库名字，例如 `dbName`.`funcName`。

当前版本中，使用原生UDF时还需要将向量化关闭  
```
set enable_vectorized_engine = false;
```


## 删除 UDF函数

当你不再需要 UDF 函数时，你可以通过下述命令来删除一个 UDF 函数, 可以参考 `DROP FUNCTION`。

