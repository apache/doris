---
{
    "title": "Remote User Defined Aggregation Function Service",
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

# User Defined Function Rpc

Remote UDAF Service The Remote UDAF Service can be accessed through RPC to implement the execution of user-defined functions. Compared with Native UDAF implementations, Remote UDAF Service has the following advantages and limitations:
1. The advantage
   * Cross-language: UDAF services can be written in all languages supported by Protobuf.
   * Security: UDAF execution failure or crash only affects the UDAF Service and does not cause the Doris process to crash.
   * Flexibility: Any other Service or library class can be invoked within a UDAF Service to meet a wider variety of business requirements.

2. Restrictions on use
   * Performance: Compared to Native UDAFs, UDAF services incur extra network overhead and thus have much lower performance than Native UDAFs. At the same time, the implementation of the UDAF Service also affects the execution efficiency of the function. Users need to deal with problems such as high concurrency and thread safety by themselves.
   * Single line mode and batch mode: Doris's original query execution framework based on row memory would execute one UDAF RPC call for each row of data, so the execution efficiency was very poor. However, under the new vectorization execution framework, one UDAF RPC call would be executed for each batch of data, so the performance was significantly improved. In actual tests, the performance of Remote UDAF based on vectorization and batch processing is similar to that of Native UDAF based on rowmemory, which can be used for reference.

## Write UDAF functions

This section describes how to develop a Remote RPC Service. Samples for the Java version are provided under `samples/doris-demo/UDAF-demo/` for your reference.

### Copy the proto file

Copy gensrc/proto/function_service.proto and gensrc/proto/types.proto to Rpc service

- function_service.proto
  - PFunctionCallRequest
    - function_name：The function name, corresponding to the symbol specified when the function was created
    - args：The parameters passed by the method
    - context：Querying context Information
  - PFunctionCallResponse
    - result：Return result
    - status：Return Status, 0 indicates normal
  - PCheckFunctionRequest
    - function：Function related information
    - match_type：Matching type
  - PCheckFunctionResponse
    - status：Return status, 0 indicates normal

### Generated interface

Use protoc generate code, and specific parameters are viewed using protoc -h

### Implementing an interface

The following three methods need to be implemented
- fnCall：Used to write computational logic
- checkFn：Used to verify function names, parameters, and return values when creating UDAFs
- handShake：Used for interface probe

## Create UDAF

Currently, UDAF and UDTF are not supported

```sql
CREATE FUNCTION 
name ([,...])
[RETURNS] rettype
PROPERTIES (["key"="value"][,...])	
```
Instructions:

1. PROPERTIES中`symbol`Represents the name of the method passed by the RPC call, which must be set。
2. PROPERTIES中`object_file`Represents the RPC service address. Currently, a single address and a cluster address in BRPC-compatible format are supported. Refer to the cluster connection mode[Format specification](https://github.com/apache/incubator-brpc/blob/master/docs/cn/client.md#%E8%BF%9E%E6%8E%A5%E6%9C%8D%E5%8A%A1%E9%9B%86%E7%BE%A4)。
3. PROPERTIES中`type`Indicates the UDAF call type, which is Native by default. Rpc is transmitted when Rpc UDAF is used。

Sample:
```sql
CREATE AGGREGATE FUNCTION rpc_sum(int) RETURNS int PROPERTIES (

"TYPE"="RPC",

"OBJECT_FILE"="127.0.0.1:9000",

"update_fn"="rpc_sum_update",

"merge_fn"="rpc_sum_merge",

"finalize_fn"="rpc_sum_finalize"
);
```

## Use UDAF

Users must have the `SELECT` permission of the corresponding database to use UDAF/UDAF.

The use of UDAF is consistent with ordinary function methods. The only difference is that the scope of built-in functions is global, and the scope of UDAF is internal to DB. When the link session is inside the data, directly using the UDAF name will find the corresponding UDAF inside the current DB. Otherwise, the user needs to display the specified UDAF database name, such as `dbName`.`funcName`.

## Delete UDAF

When you no longer need UDAF functions, you can delete a UDAF function by the following command, you can refer to `DROP FUNCTION`.

## Example
Examples of rpc server implementations and cpp/java/python languages are provided in the `samples/doris-demo/` directory. See the `README.md` in each directory for details on how to use it.