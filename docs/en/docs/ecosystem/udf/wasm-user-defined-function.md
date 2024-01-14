---
{
"title": "WASM UDF",
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

# WASM UDF

<version since="1.2.0">

WASM UDF

</version>
WASM (WebAssembly) UDF provides a way to write custom functions using WebAssembly in Doris. WASM UDF has some unique advantages and limitations compared to Java UDF:

1. The advantages:

* Cross-language compatibility: WebAssembly is a cross-language binary instruction set that allows compilation into UDFs using multiple programming languages. This way, users can choose a language they are familiar with without being restricted to a specific language.
* Performance: WebAssembly generally executes faster than interpreted languages. By compiling UDFs to WebAssembly, users can achieve higher performance, especially when it comes to computationally intensive tasks.
* Lightweight: WebAssembly's binary format is relatively small, so the overhead of transmission and loading is low, making it more portable in distributed computing environments.

2. Restrictions on use:

* Ecosystem: WebAssembly's ecosystem is relatively new and lacks some mature libraries and tools. Users may need to write some tools themselves to meet their own needs.
* Debugging difficulty: Debugging WebAssembly is relatively complex because it is a low-level binary format. Users may need more tools and skills to debug and optimize their UDFs than with high-level languages.
* Security: Although WebAssembly is designed to run in a sandbox, users still need to be cautious about potential security risks. When loading a UDF you need to make sure it is from a trusted source.

## Create UDF

```sql
CREATE FUNCTION 
name ([,...])
[RETURNS] rettype
PROPERTIES (["key"="value"][,...])	
```
Instructions:

1. `symbol` in properties represents the class name containing UDF classes. This parameter must be set.
2. The Wat file containing UDF represented by `file` in properties must be set.
3. The UDF call type represented by `type` in properties is native by default. When using wasm UDF, it is transferred to `WASM_UDF`.
4. In PROPERTIES `always_nullable` indicates whether there may be a NULL value in the UDF return result. It is an optional parameter. The default value is true.
5. `name`: A function belongs to a DB and name is of the form`dbName`.`funcName`. When `dbName` is not explicitly specified, the db of the current session is used`dbName`.

Sample：
```sql
CREATE FUNCTION wasm_udf_add_one(int) RETURNS int PROPERTIES (
    "file"="file:///path/to/wasm-udf-demo.wat,
    "symbol"="add",
    "always_nullable"="true",
    "type"="WASM_UDF"
);
```

```
* "file"=" http://IP:port/udf.wat ", you can also use http to download wat file in a multi machine environment.

* The "always_nullable" is optional attribute, if there is special treatment for the NULL value in the calculation, it is determined that the result will not return NULL, and it can be set to false, so that the performance may be better in the whole calculation process.

* If you use the local path method, the wat file that the database driver depends on, the FE and BE nodes must be placed here
## Create UDAF

Currently, UDTF are not supported.

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

## Use UDF

Users must have the `SELECT` permission of the corresponding database to use UDF/UDAF.

The use of UDF is consistent with ordinary function methods. The only difference is that the scope of built-in functions is global, and the scope of UDF is internal to DB. When the link session is inside the data, directly using the UDF name will find the corresponding UDF inside the current DB. Otherwise, the user needs to display the specified UDF database name, such as `dbName`.`funcName`.

## Delete UDF

When you no longer need UDF functions, you can delete a UDF function by the following command, you can refer to `DROP FUNCTION`.

## Instructions
1. Data types other than INT, FLOAT are not supported.

