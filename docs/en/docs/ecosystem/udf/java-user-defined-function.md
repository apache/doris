---
{
"title": "Java UDF",
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

### Type correspondence

|Type|UDF Argument Type|
|----|---------|
|Bool|Boolean|
|TinyInt|Byte|
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

## Write UDF functions

This section mainly introduces how to develop a Java UDF. Samples for the Java version are provided under `samples/doris-demo/java-udf-demo/` for your reference, Check it out [here](https://github.com/apache/incubator-doris/tree/master/samples/doris-demo/java-udf-demo)

To use Java UDF, the main entry of UDF must be the `evaluate` function. This is consistent with other engines such as Hive. In the example of `AddOne`, we have completed the operation of adding an integer as the UDF.

It is worth mentioning that this example is not only the Java UDF supported by Doris, but also the UDF supported by Hive, that's to say, for users, Hive UDF can be directly migrated to Doris.

## Create UDF

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
4. In PROPERTIES `always_nullable` indicates whether there may be a NULL value in the UDF return result. It is an optional parameter. The default value is true.
5. `name`: A function belongs to a DB and name is of the form`dbName`.`funcName`. When `dbName` is not explicitly specified, the db of the current session is used`dbName`.

Sample：
```sql
CREATE FUNCTION java_udf_add_one(int) RETURNS int PROPERTIES (
    "file"="file:///path/to/java-udf-demo-jar-with-dependencies.jar",
    "symbol"="org.apache.doris.udf.AddOne",
    "always_nullable"="true",
    "type"="JAVA_UDF"
);
```
* "file"=" http://IP:port/udf -code. Jar ", you can also use http to download jar packages in a multi machine environment.

* The "always_nullable" is optional attribute, if there is special treatment for the NULL value in the calculation, it is determined that the result will not return NULL, and it can be set to false, so that the performance may be better in the whole calculation process.
## Create UDAF
<br/>
When using Java code to write UDAF, there are some functions that must be implemented (mark required) and an inner class State, which will be explained with a specific example below.
The following SimpleDemo will implement a simple function similar to sum, the input parameter is INT, and the output parameter is INT

```JAVA
package org.apache.doris.udf;

public class SimpleDemo {
    //Need an inner class to store data
    /*required*/  
    public static class State {
        /*some variables if you need */
        public int sum = 0;
    }

    /*required*/
    public State create() {
        /* here could do some init work if needed */
        return new State();
    }

    /*required*/
    public void destroy(State state) {
      /* here could do some destroy work if needed */
    }

    /*required*/ 
    //first argument is State, then other types your input
    public void add(State state, Integer val) {
      /* here doing update work when input data*/
        if (val != null) {
            state.sum += val;
        }
    }

    /*required*/
    public void serialize(State state, DataOutputStream out) {
      /* serialize some data into buffer */
        out.writeInt(state.sum);
    }

    /*required*/
    public void deserialize(State state, DataInputStream in) {
      /* deserialize get data from buffer before you put */
        int val = in.readInt();
        state.sum = val;
    }

    /*required*/
    public void merge(State state, State rhs) {
      /* merge data from state */
        state.sum += rhs.sum;
    }

    /*required*/
    //return Type you defined
    public Integer getValue(State state) {
      /* return finally result */
        return state.sum;
    }
}

```

```sql
CREATE AGGREGATE FUNCTION simple_sum(INT) RETURNS INT PROPERTIES (
    "file"="file:///pathTo/java-udaf.jar",
    "symbol"="org.apache.doris.udf.SimpleDemo",
    "always_nullable"="true",
    "type"="JAVA_UDF"
);
```

Currently, UDTF are not supported.

<br/>

## Use UDF

Users must have the `SELECT` permission of the corresponding database to use UDF/UDAF.

The use of UDF is consistent with ordinary function methods. The only difference is that the scope of built-in functions is global, and the scope of UDF is internal to DB. When the link session is inside the data, directly using the UDF name will find the corresponding UDF inside the current DB. Otherwise, the user needs to display the specified UDF database name, such as `dbName`.`funcName`.

## Delete UDF

When you no longer need UDF functions, you can delete a UDF function by the following command, you can refer to `DROP FUNCTION`.

## Example
Examples of Java UDF are provided in the `samples/doris-demo/java-udf-demo/` directory. See the `README.md` in each directory for details on how to use it, Check it out [here](https://github.com/apache/incubator-doris/tree/master/samples/doris-demo/java-udf-demo)

## Instructions
1. Complex data types (HLL, bitmap) are not supported.
2. Currently, users are allowed to specify the maximum heap size of the JVM themselves. The configuration item is jvm_ max_ heap_ size.
3. The udf of char type needs to use the String type when creating a function.
4. Due to the problem that the jvm loads classes with the same name, do not use multiple classes with the same name as udf implementations at the same time. If you want to update the udf of a class with the same name, you need to restart be to reload the classpath.

