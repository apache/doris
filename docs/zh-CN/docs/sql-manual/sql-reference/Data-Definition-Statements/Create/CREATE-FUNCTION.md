---
{
    "title": "CREATE-FUNCTION",
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

## CREATE-FUNCTION

### Name

CREATE FUNCTION

### Description

此语句创建一个自定义函数。执行此命令需要用户拥有 `ADMIN` 权限。

如果 `function_name` 中包含了数据库名字，那么这个自定义函数会创建在对应的数据库中，否则这个函数将会创建在当前会话所在的数据库。新函数的名字与参数不能够与当前命名空间中已存在的函数相同，否则会创建失败。但是只有名字相同，参数不同是能够创建成功的。

语法：

```sql
CREATE [GLOBAL] [AGGREGATE] [ALIAS] FUNCTION function_name
    (arg_type [, ...])
    [RETURNS ret_type]
    [INTERMEDIATE inter_type]
    [WITH PARAMETER(param [,...]) AS origin_function]
    [PROPERTIES ("key" = "value" [, ...]) ]
```

参数说明：

-  `GLOBAL`: 如果有此项，表示的是创建的函数是全局范围内生效。

-  `AGGREGATE`: 如果有此项，表示的是创建的函数是一个聚合函数。


-  `ALIAS`：如果有此项，表示的是创建的函数是一个别名函数。


 		如果没有上述两项，表示创建的函数是一个标量函数

-  `function_name`: 要创建函数的名字, 可以包含数据库的名字。比如：`db1.my_func`。


-  `arg_type`: 函数的参数类型，与建表时定义的类型一致。变长参数时可以使用`, ...`来表示，如果是变长类型，那么变长部分参数的类型与最后一个非变长参数类型一致。

   **注意**：`ALIAS FUNCTION` 不支持变长参数，且至少有一个参数。

-  `ret_type`: 对创建新的函数来说，是必填项。如果是给已有函数取别名则可不用填写该参数。


-  `inter_type`: 用于表示聚合函数中间阶段的数据类型。


-  `param`：用于表示别名函数的参数，至少包含一个。


-  `origin_function`：用于表示别名函数对应的原始函数。

- `properties`: 用于设定函数相关属性，能够设置的属性包括：	

  - `file`: 表示的包含用户UDF的jar包，当在多机环境时，也可以使用http的方式下载jar包。这个参数是必须设定的。

  - `symbol`: 表示的是包含UDF类的类名。这个参数是必须设定的

  - `type`: 表示的 UDF 调用类型，默认为 Native，使用 Java UDF时传 JAVA_UDF。

  - `always_nullable`：表示的 UDF 返回结果中是否有可能出现NULL值，是可选参数，默认值为true。


### Example

1. 创建一个自定义UDF函数

   ```sql
   CREATE FUNCTION java_udf_add_one(int) RETURNS int PROPERTIES (
       "file"="file:///path/to/java-udf-demo-jar-with-dependencies.jar",
       "symbol"="org.apache.doris.udf.AddOne",
       "always_nullable"="true",
       "type"="JAVA_UDF"
   );
   ```


2. 创建一个自定义UDAF函数

   ```sql
   CREATE AGGREGATE FUNCTION simple_sum(INT) RETURNS INT PROPERTIES (
       "file"="file:///pathTo/java-udaf.jar",
       "symbol"="org.apache.doris.udf.demo.SimpleDemo",
       "always_nullable"="true",
       "type"="JAVA_UDF"
   );
   ```

3. 创建一个自定义别名函数

   ```sql
   CREATE ALIAS FUNCTION id_masking(INT) WITH PARAMETER(id)  AS CONCAT(LEFT(id, 3), '****', RIGHT(id, 4));
   ```

4. 创建一个全局自定义别名函数

   ```sql
   CREATE GLOBAL ALIAS FUNCTION id_masking(INT) WITH PARAMETER(id) AS CONCAT(LEFT(id, 3), '****', RIGHT(id, 4));
   ```` 
   
### Keywords

    CREATE, FUNCTION

### Best Practice

