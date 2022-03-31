---
{
    "title": "Native User Defined Function",
    "language": "en"
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

# Native User Defined Function
UDF is mainly suitable for scenarios where the analytical capabilities that users need do not possess. Users can implement custom functions according to their own needs, and register with Doris through the UDF framework to expand Doris' capabilities and solve user analysis needs.

There are two types of analysis requirements that UDF can meet: UDF and UDAF. UDF in this article refers to both.

1. UDF: User-defined function, this function will operate on a single line and output a single line result. When users use UDFs for queries, each row of data will eventually appear in the result set. Typical UDFs are string operations such as concat().
2. UDAF: User-defined aggregation function. This function operates on multiple lines and outputs a single line of results. When the user uses UDAF in the query, each group of data after grouping will finally calculate a value and expand the result set. A typical UDAF is the set operation sum(). Generally speaking, UDAF will be used together with group by.

This document mainly describes how to write a custom UDF function and how to use it in Doris.

If users use the UDF function and extend Doris' function analysis, and want to contribute their own UDF functions back to the Doris community for other users, please see the document [Contribute UDF](./contribute_udf.md).

## Writing UDF functions

Before using UDF, users need to write their own UDF functions under Doris' UDF framework. In the `contrib/udf/src/udf_samples/udf_sample.h|cpp` file is a simple UDF Demo.

Writing a UDF function requires the following steps.

### Writing functions

Create the corresponding header file and CPP file, and implement the logic you need in the CPP file. Correspondence between the implementation function format and UDF in the CPP file.

Users can put their own source code in a folder. Taking udf_sample as an example, the directory structure is as follows:

```
└── udf_samples
  ├── uda_sample.cpp
  ├── uda_sample.h
  ├── udf_sample.cpp
  └── udf_sample.h
```

#### Non-variable parameters

For UDFs with non-variable parameters, the correspondence between the two is straightforward.
For example, the UDF of `INT MyADD(INT, INT)` will correspond to `IntVal AddUdf(FunctionContext* context, const IntVal& arg1, const IntVal& arg2)`.

1. `AddUdf` can be any name, as long as it is specified when creating UDF.
2. The first parameter in the implementation function is always `FunctionContext*`. The implementer can obtain some query-related content through this structure, and apply for some memory to be used. The specific interface used can refer to the definition in `udf/udf.h`.
3. In the implementation function, the second parameter needs to correspond to the UDF parameter one by one, for example, `IntVal` corresponds to `INT` type. All types in this part must be referenced with `const`.
4. The return parameter must correspond to the type of UDF parameter.

#### variable parameter

For variable parameters, you can refer to the following example, corresponding to UDF`String md5sum(String, ...)`
The implementation function is `StringVal md5sumUdf(FunctionContext* ctx, int num_args, const StringVal* args)`

1. `md5sumUdf` can also be changed arbitrarily, just specify it when creating.
2. The first parameter is the same as the non-variable parameter function, and the passed in is a `FunctionContext*`.
3. The variable parameter part consists of two parts. First, an integer is passed in, indicating that there are several parameters behind. An array of variable parameter parts is passed in later.

#### Type correspondence

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


## Compile UDF function

Since the UDF implementation relies on Doris' UDF framework, the first step in compiling UDF functions is to compile Doris, that is, the UDF framework.

After the compilation is completed, the static library file of the UDF framework will be generated. Then introduce the UDF framework dependency and compile the UDF.

### Compile Doris

Running `sh build.sh` in the root directory of Doris will generate a static library file of the UDF framework `headers|libs` in `output/udf/`

```
├── output
│ └── udf
│ ├── include
│ │ ├── uda_test_harness.h
│ │ └── udf.h
│ └── lib
│ └── libDorisUdf.a

```

### Writing UDF compilation files

1. Prepare thirdparty

    The thirdparty folder is mainly used to store thirdparty libraries that users' UDF functions depend on, including header files and static libraries. It must contain the two files `udf.h` and `libDorisUdf.a` in the dependent Doris UDF framework.

    Taking udf_sample as an example here, the source code is stored in the user's own `udf_samples` directory. Create a thirdparty folder in the same directory to store the static library. The directory structure is as follows:

    ```
    ├── thirdparty
    │  │── include
    │  │ └── udf.h
    │  └── lib
    │    └── libDorisUdf.a
    └── udf_samples

    ```

    `udf.h` is the UDF frame header file. The storage path is `doris/output/udf/include/udf.h`. Users need to copy the header file in the Doris compilation output to their include folder of `thirdparty`.

    `libDorisUdf.a` is a static library of UDF framework. After Doris is compiled, the file is stored in `doris/output/udf/lib/libDorisUdf.a`. The user needs to copy the file to the lib folder of his `thirdparty`.

    *Note: The static library of UDF framework will not be generated until Doris is compiled.

2. Prepare to compile UDF's CMakeFiles.txt

    CMakeFiles.txt is used to declare how UDF functions are compiled. Stored in the source code folder, level with user code. Here, taking udf_samples as an example, the directory structure is as follows:

    ```
    ├── thirdparty
    └── udf_samples
      ├── CMakeLists.txt
      ├── uda_sample.cpp
      ├── uda_sample.h
      ├── udf_sample.cpp
      └── udf_sample.h
    ```

    + Need to show declaration reference `libDorisUdf.a`
    + Declare `udf.h` header file location


    Take udf_sample as an example

    ```
    # Include udf
    include_directories(thirdparty/include)

    # Set all libraries
    add_library(udf STATIC IMPORTED)
    set_target_properties(udf PROPERTIES IMPORTED_LOCATION thirdparty/lib/libDorisUdf.a)

    # where to put generated libraries
    set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/udf_samples")

    # where to put generated binaries
    set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/udf_samples")

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

    If the user's UDF function also depends on other thirdparty libraries, you need to declare include, lib, and add dependencies in `add_library`.

The complete directory structure after all files are prepared is as follows:

```
    ├── thirdparty
    │ │── include
    │ │ └── udf.h
    │ └── lib
    │ └── libDorisUdf.a
    └── udf_samples
      ├── CMakeLists.txt
      ├── uda_sample.cpp
      ├── uda_sample.h
      ├── udf_sample.cpp
      └── udf_sample.h
```

Prepare the above files and you can compile UDF directly

### Execute compilation

Create a build folder under the udf_samples folder to store the compilation output.

Run the command `cmake ../` in the build folder to generate a Makefile, and execute make to generate the corresponding dynamic library.

```
├── thirdparty
├── udf_samples
  └── build
```

### Compilation result

After the compilation is completed, the UDF dynamic link library is successfully generated. Under `build/src/`, taking udf_samples as an example, the directory structure is as follows:

```
├── thirdparty
├── udf_samples
  └── build
    └── src
      └── udf_samples
        ├── libudasample.so
        └── libudfsample.so

```

## Create UDF function

After following the above steps, you can get the UDF dynamic library (that is, the `.so` file in the compilation result). You need to put this dynamic library in a location that can be accessed through the HTTP protocol.

Then log in to the Doris system and create a UDF function in the mysql-client through the `CREATE FUNCTION` syntax. You need to have ADMIN authority to complete this operation. At this time, there will be a UDF created in the Doris system.

```
CREATE [AGGREGATE] FUNCTION
name ([argtype][,...])
[RETURNS] rettype
PROPERTIES (["key"="value"][,...])
```
Description:

1. "Symbol" in PROPERTIES means that the symbol corresponding to the entry function is executed. This parameter must be set. You can get the corresponding symbol through the `nm` command, for example, `_ZN9doris_udf6AddUdfEPNS_15FunctionContextERKNS_6IntValES4_` obtained by `nm libudfsample.so | grep AddUdf` is the corresponding symbol.
2. The object_file in PROPERTIES indicates where it can be downloaded to the corresponding dynamic library. This parameter must be set.
3. name: A function belongs to a certain DB, and the name is in the form of `dbName`.`funcName`. When `dbName` is not explicitly specified, the db where the current session is located is used as `dbName`.

For specific use, please refer to `CREATE FUNCTION` for more detailed information.

## Use UDF

Users must have the `SELECT` permission of the corresponding database to use UDF/UDAF.

The use of UDF is consistent with ordinary function methods. The only difference is that the scope of built-in functions is global, and the scope of UDF is internal to DB. When the link session is inside the data, directly using the UDF name will find the corresponding UDF inside the current DB. Otherwise, the user needs to display the specified UDF database name, such as `dbName`.`funcName`.


## Delete UDF

When you no longer need UDF functions, you can delete a UDF function by the following command, you can refer to `DROP FUNCTION`.
