---
{
    "title": "User Define Function",
    "language": "en"
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

Users can extend Doris's capabilities through UDF mechanisms. Through this document, users can create their own UDF.

## Writing UDF functions

Before using UDF, users need to write their own UDF functions in Doris's UDF framework. In the `be/src/udf_samples/udf_sample.h | cpp` file, it is a simple UDF Demo.

Writing a UDF function requires the following steps.

### Writing functions

Create the corresponding header file, CPP file, and implement the logic you need in the CPP file. The corresponding relationship between the format of implementation function in CPP file and UDF.

#### Non-variable parameters

For UDF with non-variable parameters, the corresponding relationship between them is very direct.
For example, `INT MyADD'(INT, INT) ` UDF corresponds to `IntVal AddUdf(FunctionContext* context, const IntVal & arg1, const IntVal & arg2)`.

1. `AddUdf` can be any name, as long as it is specified when UDF is created.
2. The first parameter in the implementation function is always `FunctionContext*`. The implementer can obtain some query-related content and apply for some memory to be used through this structure. Specific interfaces can be defined in `udf/udf.h`.
3. Implementing functions from the second parameter requires one-to-one correspondence with UDF parameters, such as `IntVal` corresponding to `INT` type. All types in this section are referenced by `const`.
4. Return parameters should correspond to the type of UDF parameters.

#### Variable parameters

For variable parameters, see the following example, UDF `String md5sum (String,...)` corresponds to
`StringVal md5sumUdf (FunctionContext * ctx, int num args, const StringVal * args)`

1. The `md5sumUdf` can also be changed at will. It can be specified at the time of creation.
2. The first parameter, like a non-variable parameter function, is passed in a `FunctionContext*`.
3. The variable parameter part consists of two parts. First, an integer is passed in, which shows that there are several parameters. Later, an array of variable parameter parts is passed in.

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

## Compiling UDF functions

### Compile Doris

Executing `sh build.sh` in the Doris root directory generates the corresponding `headers|libs` in `output/udf/`

### Edit CMakeLists.txt

Based on the `headers | libs` generated in the previous step, users can introduce the dependency using tools such as `CMakeLists`; in `CMakeLists`, dynamic libraries can be added by adding `-I|L` to `CMAKE_CXX_FLAGS`, respectively. For example, in `be/src/udf_samples/CMakeLists.txt`, a `udf sample` dynamic library is added using `add_library` (udfsample SHARED udf_sample.cpp) `target_link_libraries`(udfsample -static-libstdc++ -static-libgcc). You need to write down all the source files involved later (no header files included).

### Execute compilation

Create a `build` directory under this directory and execute `cmake ../` generate `Makefile` under `build`, and execute `make` to generate corresponding dynamic libraries.

## Create UDF functions

Through the above steps, you can get a dynamic library. You need to put this dynamic library in a location that can be accessed through the HTTP protocol. Then execute the create UDF function to create a UDF inside the Doris system. You need AMDIN privileges to do this.

```
CREATE [AGGREGATE] FUNCTION 
	name ([argtype][,...])
	[RETURNS] rettype
	PROPERTIES (["key"="value"][,...])
```
Explain:

1. In PROPERTIES, `symbol` denotes the corresponding symbol for the execution of the entry function, which must be set. You can get the corresponding symbol by the `nm` command, such as `nm libudfsample.so`, `grep AddUdf`, `ZN9doris_udf6AddUdfEPNS_15FunctionContextERKNS_6IntValES4`.
2. In PROPERTIES, `object_file` denotes where to download to the corresponding dynamic library. This parameter must be set.
3. name: A function belongs to a DB in the form of `dbName`. `funcName`. When `dbName` is not specified explicitly, the DB where the current session is located is used as `dbName`.

For more details, see `CREATE FUNCTION`.

## Using UDF

Users using UDF/UDAF must have `SELECT` privileges for the corresponding database.

UDF is used in the same way as normal functions. The only difference is that the scope of built-in functions is global, while the scope of UDF is internal to DB. When the link session is inside the data, using the UDF name directly will find the corresponding UDF within the current DB. Otherwise, the user needs to display the database name of the specified UDF, such as `dbName`. `funcName`.


## Delete UDF functions

When you no longer need UDF functions, you can delete a UDF function by using the following command, referring to `DROP FUNCTION`.
