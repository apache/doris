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
CREATE FUNCTION 
	name ([,...])
	[RETURNS] rettype
	PROPERTIES (["key"="value"][,...]) 
````

Parameter Description:

- `name`: A function belongs to a certain DB, and the name is in the form of `dbName`.`funcName`. When `dbName` is not specified explicitly, the db where the current session is located is used as `dbName`.


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
