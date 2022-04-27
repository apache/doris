---
{
    "title": "[Experimental] Java UDF",
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

# Java UDF

Java UDF provides users with a Java interface written in UDF to facilitate the execution of user-defined functions in Java language. Compared with native UDF implementation, Java UDF has the following advantages and limitations:
1. The advantages
  * Compatibility: Using Java UDF can be compatible with different Doris versions, so when upgrading Doris version, Java UDF does not need additional migration. At the same time, Java UDF also follows the same programming specifications as hive / spark and other engines, so that users can directly move Hive / Spark UDF jar to Doris.
  * Security: The failure or crash of Java UDF execution will only cause the JVM to report an error, not the Doris process to crash.
  * Flexibility: In Java UDF, users can package the third-party dependencies together in the user jar.

2. Restrictions on use
  * Performance: Compared with native UDF, Java UDF will bring additional JNI overhead, but through batch execution, we have minimized the JNI overhead as much as possible.
  * Vectorized engine: Java UDF is only supported on vectorized engine now.

## Write UDF functions

This section mainly introduces how to develop a Java UDF. Samples for the Java version are provided under `samples/doris-demo/java-udf-demo/` for your reference.

To use Java UDF, the main entry of UDF must be the `evaluate` function. This is consistent with other engines such as Hive. In the example of `AddOne`, we have completed the operation of adding an integer as the UDF.

It is worth mentioning that this example is not only the Java UDF supported by Doris, but also the UDF supported by Hive, that's to say, for users, Hive UDF can be directly migrated to Doris.

## Create UDF

Currently, UDAF and UDTF are not supported.

```sql
CREATE FUNCTION 
name ([,...])
[RETURNS] rettype
PROPERTIES (["key"="value"][,...])	
```
Instructions:

1. `symbol` in properties represents the class name containing UDF classes. This parameter must be set.
2. The jar package containing UDF represented by `file` in properties must be set.
3. The UDF call type represented by `type` in properties is native by default. When using java UDF, it is transferred to `Java_UDF`.
4. `name`: A function belongs to a DB and name is of the form`dbName`.`funcName`. When `dbName` is not explicitly specified, the db of the current session is used`dbName`.

Sample:
```sql
CREATE FUNCTION java_udf_add_one(int) RETURNS int PROPERTIES (
    "file"="file:///path/to/java-udf-demo-jar-with-dependencies.jar",
    "symbol"="org.apache.doris.udf.AddOne",
    "type"="JAVA_UDF"
);
```

## Use UDF

Users must have the `SELECT` permission of the corresponding database to use UDF/UDAF.

The use of UDF is consistent with ordinary function methods. The only difference is that the scope of built-in functions is global, and the scope of UDF is internal to DB. When the link session is inside the data, directly using the UDF name will find the corresponding UDF inside the current DB. Otherwise, the user needs to display the specified UDF database name, such as `dbName`.`funcName`.

## Delete UDF

When you no longer need UDF functions, you can delete a UDF function by the following command, you can refer to `DROP FUNCTION`.

## Example
Examples of Java UDF are provided in the `samples/doris-demo/java-udf-demo/` directory. See the `README.md` in each directory for details on how to use it.

## Unsupported Use Case
At present, Java UDF is still in the process of continuous development, so some features are **not completed**.
1. Complex data types (date, HLL, bitmap) are not supported.
2. Memory management and statistics of JVM and Doris have not been unified.
