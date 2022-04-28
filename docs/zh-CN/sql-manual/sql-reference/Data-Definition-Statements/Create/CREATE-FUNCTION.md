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
CREATE [AGGREGATE] [ALIAS] FUNCTION function_name
    (arg_type [, ...])
    [RETURNS ret_type]
    [INTERMEDIATE inter_type]
    [WITH PARAMETER(param [,...]) AS origin_function]
    [PROPERTIES ("key" = "value" [, ...]) ]
```

参数说明：

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

- `properties`: 用于设定聚合函数和标量函数相关属性，能够设置的属性包括：	

  - `object_file`: 自定义函数动态库的URL路径，当前只支持 HTTP/HTTPS 协议，此路径需要在函数整个生命周期内保持有效。此选项为必选项

  - `symbol`: 标量函数的函数签名，用于从动态库里面找到函数入口。此选项对于标量函数是必选项

  - `init_fn`: 聚合函数的初始化函数签名。对于聚合函数是必选项

  - `update_fn`: 聚合函数的更新函数签名。对于聚合函数是必选项

  - `merge_fn`: 聚合函数的合并函数签名。对于聚合函数是必选项

  - `serialize_fn`: 聚合函数的序列化函数签名。对于聚合函数是可选项，如果没有指定，那么将会使用默认的序列化函数

  - `finalize_fn`: 聚合函数获取最后结果的函数签名。对于聚合函数是可选项，如果没有指定，将会使用默认的获取结果函数

  - `md5`: 函数动态链接库的MD5值，用于校验下载的内容是否正确。此选项是可选项

  - `prepare_fn`: 自定义函数的prepare函数的函数签名，用于从动态库里面找到prepare函数入口。此选项对于自定义函数是可选项

  - `close_fn`: 自定义函数的close函数的函数签名，用于从动态库里面找到close函数入口。此选项对于自定义函数是可选项     

### Example

1. 创建一个自定义标量函数

   ```sql
   CREATE FUNCTION my_add(INT, INT) RETURNS INT PROPERTIES (
   	"symbol" = "_ZN9doris_udf6AddUdfEPNS_15FunctionContextERKNS_6IntValES4_",
   	"object_file" = "http://host:port/libmyadd.so"
   );
   ```

2. 创建一个有prepare/close函数的自定义标量函数

   ```sql
   CREATE FUNCTION my_add(INT, INT) RETURNS INT PROPERTIES (
   	"symbol" = "_ZN9doris_udf6AddUdfEPNS_15FunctionContextERKNS_6IntValES4_",
   	"prepare_fn" = 	"_ZN9doris_udf14AddUdf_prepareEPNS_15FunctionContextENS0_18FunctionStateScopeE",
   	"close_fn" = "_ZN9doris_udf12AddUdf_closeEPNS_15FunctionContextENS0_18FunctionStateScopeE",
   	"object_file" = "http://host:port/libmyadd.so"
   );
   ```

3. 创建一个自定义聚合函数

    ```sql
   CREATE AGGREGATE FUNCTION my_count (BIGINT) RETURNS BIGINT PROPERTIES (
            "init_fn"="_ZN9doris_udf9CountInitEPNS_15FunctionContextEPNS_9BigIntValE",
            "update_fn"="_ZN9doris_udf11CountUpdateEPNS_15FunctionContextERKNS_6IntValEPNS_9BigIntValE",
            "merge_fn"="_ZN9doris_udf10CountMergeEPNS_15FunctionContextERKNS_9BigIntValEPS2_",
            "finalize_fn"="_ZN9doris_udf13CountFinalizeEPNS_15FunctionContextERKNS_9BigIntValE",
            "object_file"="http://host:port/libudasample.so"
   );
   ```


4. 创建一个变长参数的标量函数

   ```sql
   CREATE FUNCTION strconcat(varchar, ...) RETURNS varchar properties (
   	"symbol" = "_ZN9doris_udf6StrConcatUdfEPNS_15FunctionContextERKNS_6IntValES4_",
   	"object_file" = "http://host:port/libmyStrConcat.so"
   );
   ```

5. 创建一个自定义别名函数

   ```sql
   CREATE ALIAS FUNCTION id_masking(INT) WITH PARAMETER(id)  AS CONCAT(LEFT(id, 3), '****', RIGHT(id, 4));
   ```

### Keywords

    CREATE, FUNCTION

### Best Practice

