---
{
    "title": "CREATE-FUNCTION",
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

## CREATE-FUNCTION

### Name

CREATE FUNCTION

### Description

This statement creates a custom function. Executing this command requires the user to have `ADMIN` privileges.

If `function_name` contains the database name, then the custom function will be created in the corresponding database, otherwise the function will be created in the database where the current session is located. The name and parameters of the new function cannot be the same as the existing functions in the current namespace, otherwise the creation will fail. But only with the same name and different parameters can be created successfully.

grammar:

```sql
CREATE [GLOBAL] [AGGREGATE] [ALIAS] FUNCTION function_name
    (arg_type [, ...])
    [RETURNS ret_type]
    [INTERMEDIATE inter_type]
    [WITH PARAMETER(param [,...]) AS origin_function]
    [PROPERTIES ("key" = "value" [, ...]) ]
````

Parameter Description:

- `GLOBAL`: If there is this item, it means that the created function is a global function.

- `AGGREGATE`: If there is this item, it means that the created function is an aggregate function.


- `ALIAS`: If there is this item, it means that the created function is an alias function.


 If the above two items are absent, it means that the created function is a scalar function

- `function_name`: The name of the function to be created, which can include the name of the database. For example: `db1.my_func`.


- `arg_type`: The parameter type of the function, which is the same as the type defined when creating the table. Variable-length parameters can be represented by `, ...`. If it is a variable-length type, the type of the variable-length parameter is the same as that of the last non-variable-length parameter.

  **NOTE**: `ALIAS FUNCTION` does not support variable-length arguments and must have at least one argument.

- `ret_type`: Required for creating new functions. If you are aliasing an existing function, you do not need to fill in this parameter.


- `inter_type`: The data type used to represent the intermediate stage of the aggregation function.


- `param`: used to represent the parameter of the alias function, including at least one.


- `origin_function`: used to represent the original function corresponding to the alias function.


- `properties`: Used to set function-related properties, the properties that can be set include:

    - `file`: Indicates the jar package containing the user UDF. In a multi-machine environment, you can also use http to download the jar package. This parameter is mandatory.

    - `symbol`: Indicates the name of the class containing the UDF class. This parameter must be set

    - `type`: Indicates the UDF call type, the default is Native, and JAVA_UDF is passed when using Java UDF.

    - `always_nullable`: Indicates whether NULL values may appear in the UDF return result, is an optional parameter, and the default value is true.


### Example

1. Create a custom UDF function

    ```sql
    CREATE FUNCTION java_udf_add_one(int) RETURNS int PROPERTIES (
        "file"="file:///path/to/java-udf-demo-jar-with-dependencies.jar",
        "symbol"="org.apache.doris.udf.AddOne",
        "always_nullable"="true",
        "type"="JAVA_UDF"
    );
    ```


2. Create a custom UDAF function

    ```sql
    CREATE AGGREGATE FUNCTION simple_sum(INT) RETURNS INT PROPERTIES (
        "file"="file:///pathTo/java-udaf.jar",
        "symbol"="org.apache.doris.udf.demo.SimpleDemo",
        "always_nullable"="true",
        "type"="JAVA_UDF"
    );
    ```

3. Create a custom alias function

    ```sql
    CREATE ALIAS FUNCTION id_masking(INT) WITH PARAMETER(id) AS CONCAT(LEFT(id, 3), '****', RIGHT(id, 4));
    ```

4. Create a global custom alias function

    ```sql
    CREATE GLOBAL ALIAS FUNCTION id_masking(INT) WITH PARAMETER(id) AS CONCAT(LEFT(id, 3), '****', RIGHT(id, 4));
    ```

### Keywords

    CREATE, FUNCTION

### Best Practice
