---
{
    "title": "User Define Function",
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

# User Define Function

Users can extend Doris' capabilities through the UDF mechanism. Through this document, users can create their own UDF.

## Writing UDF functions

Before using UDF, users need to write their own UDF functions under Doris' UDF framework. In the `custom_udf/src/udf_samples/udf_sample.h|cpp` file is a simple UDF Demo.

Writing a UDF function requires the following steps.

### Writing functions

Create the corresponding header file and CPP file, and implement the logic you need in the CPP file. Correspondence between the implementation function format and UDF in the CPP file.

#### Non-variable parameters

For UDFs with non-variable parameters, the correspondence between the two is straightforward.
For example, UTF of `INT MyADD(INT, INT)` will correspond to `IntVal AddUdf(FunctionContext* context, const IntVal& arg1, const IntVal& arg2)`

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

### Directory structure after writing

Taking udf_sample as an example here, create a `udf_samples` directory under src dir to store source code.

```

├── be
├── custom_udf
│ ├── CMakeLists.txt
│ ├── build_custom_udf.sh
│ └── src
│   └── udf_samples
│     ├── CMakeLists.txt
│     ├── uda_sample.cpp
│     ├── uda_sample.h
│     ├── udf_sample.cpp
│     └── udf_sample.h

```

## Compile UDF function

Since the function implemented by the user depends on the udf of Doris, the first step is to compile Doris when compiling the UDF function. Then compile the UDF implemented by the user.

### Compile Doris

Running `sh build.sh` in the root directory of Doris will generate the corresponding `headers|libs` in `output/udf/`

```
├── output
│   └── udf
│       ├── include
│       │   ├── uda_test_harness.h
│       │   └── udf.h
│       └── lib
│           └── libDorisUdf.a

```

### Write CMakeLists.txt for custom UDF

1. Add custom UDF compilation under `custom_udf/CMakeLists.txt`. Take udf_samples as an example

    ```
    ├── be
    ├── custom_udf
    │ ├── CMakeLists.txt
    │ └── src


    custom_udf/CMakeLists.txt
    ...
    add_subdirectory(${SRC_DIR}/udf_samples)
    ...

    ```

2. Add dependency in custom UDF. Take udf_samples as an example,

    Since the code in udf_samples does not depend on any other libraries, there is no need to declare.

    If the code depends on functions such as `StringVal` in Doris UDF, you need to declare that it depends on udf. Modify `udf_samples/CMakeFiles.txt`:

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

### Execute compilation

Run `build_custom_udf.sh` under custom_udf

```
├── be
├── custom_udf
│ ├── build_custom_udf.sh

build_custom_udf.sh --udf --clean

```

If no parameters are passed in by default, the compilation script is directly compiled and not clean. If you need to clean and then compile, you need to add the parameter `--udf --clean`

### Compilation result

After the compilation is completed, the dynamic link library is placed under `output/custom_udf/`. Taking udf_samples as an example, the directory structure is as follows:

```

├── output
│   ├── be
│   ├── custom_udf
│   │   └── lib
│   │       └── udf_samples
│   │           ├── libudasample.so
│   │           └── libudfsample.so

```

## Create UDF function

After going through the above steps, you can get a dynamic library. You need to put this dynamic library in a location that can be accessed through the HTTP protocol. Then execute the create UDF function to create a UDF inside the Doris system. You need to have AMDIN permission to complete this operation.

```
CREATE [AGGREGATE] FUNCTION
name ([argtype][,...])
[RETURNS] rettype
PROPERTIES (["key"="value"][,...])
```
Description:

1. "Symbol" in PROPERTIES means that the symbol corresponding to the entry function is executed. This parameter must be set. You can get the corresponding symbol through the `nm` command, such as `_ZN9doris_udf6AddUdfEPNS_15FunctionContextERKNS_6IntValES4_` obtained by `nm libudfsample.so | grep AddUdf` is the corresponding symbol.
2. The object_file in PROPERTIES indicates where it can be downloaded to the corresponding dynamic library. This parameter must be set.
3. name: A function belongs to a certain DB, and the name is in the form of `dbName`.`funcName`. When `dbName` is not explicitly specified, the db where the current session is located is used as `dbName`.

For specific use, please refer to `CREATE FUNCTION` for more detailed information.

## Use UDF

Users must have the `SELECT` permission of the corresponding database to use UDF/UDAF.

The use of UDF is consistent with ordinary function methods. The only difference is that the scope of built-in functions is global, and the scope of UDF is internal to DB. When the link session is inside the data, directly using the UDF name will find the corresponding UDF inside the current DB. Otherwise, the user needs to display the specified UDF database name, such as `dbName`.`funcName`.


## Delete UDF function

When you no longer need UDF functions, you can delete a UDF function by the following command, you can refer to `DROP FUNCTION`.
