---
{
    "title": "导入 Json 格式数据",
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

# 导入 Json 格式数据

Doris 从 0.12 版本开始支持 Json 格式的数据导入。

## 支持的导入方式

目前只有以下导入方式支持 Json 格式的数据导入：

* Stream Load
* Routine Load

关于以上导入方式的具体说明，请参阅相关文档。本文档主要介绍在这些导入方式中关于 Json 部分的使用说明。

## 支持的 Json 格式

当前前仅支持以下两种 Json 格式：

1. 以 Array 表示的多行数据

    以 Array 为根节点的 Json 格式。Array 中的每个元素表示要导入的一行数据，通常是一个 Object。示例如下：

    ```
    [
        { "id": 123, "city" : "beijing"},
        { "id": 456, "city" : "shanghai"},
        ...
    ]
    ```
    
    ```
    [
        { "id": 123, "city" : { "name" : "beijing", "region" : "haidian"}},
        { "id": 456, "city" : { "name" : "beijing", "region" : "chaoyang"}},
        ...
    ]
    ```
    
    这种方式通常用于 Stream Load 导入方式，以便在一批导入数据中表示多行数据。
    
    这种方式必须配合设置 `stripe_outer_array=true` 使用。Doris在解析时会将数组展开，然后依次解析其中的每一个 Object 作为一行数据。

2. 以 Object 表示的单行数据

    以 Object 为根节点的 Json 格式。整个 Object 即表示要导入的一行数据。示例如下：
    
    ```
    { "id": 123, "city" : "beijing"}
    ```
    
    ```
    { "id": 123, "city" : { "name" : "beijing", "region" : "haidian" }}
    ```
    
    这种方式通常用于 Routine Load 导入方式，如表示 Kafka 中的一条消息，即一行数据。
        
## Json Path

Doris 支持通过 Json Path 抽取 Json 中指定的数据。

**注：因为对于 Array 类型的数据，Doris 会先进行数组展开，最终按照 Object 格式进行单行处理。所以本文档之后的示例都以单个 Object 格式的 Json 数据进行说明。**

* 不指定 Json Path

    如果没有指定 Json Path，则 Doris 会默认使用表中的列名查找 Object 中的元素。示例如下：
    
    表中包含两列: `id`, `city`
    
    Json 数据如下：
    
    ```
    { "id": 123, "city" : "beijing"}
    ```
    
    则 Doris 会使用 `id`, `city` 进行匹配，得到最终数据 `123` 和 `beijing`。
    
    如果 Json 数据如下：
    
    ```
    { "id": 123, "name" : "beijing"}
    ```

    则使用 `id`, `city` 进行匹配，得到最终数据 `123` 和 `null`。
    
* 指定 Json Path

    通过一个 Json 数据的形式指定一组 Json Path。数组中的每个元素表示一个要抽取的列。示例如下：
    
    ```
    ["$.id", "$.name"]
    ```
    ```
    ["$.id.sub_id", "$.name[0]", "$.city[0]"]
    ```
    
    Doris 会使用指定的 Json Path 进行数据匹配和抽取。
    
* 匹配非基本类型

    前面的示例最终匹配到的数值都是基本类型，如整型、字符串等。Doris 当前暂不支持复合类型，如 Array、Map 等。所以当匹配到一个非基本类型时，Doris 会将该类型转换为 Json 格式的字符串，并以字符串类型进行导入。示例如下：
    
    Json 数据为：
    
    ```
    { "id": 123, "city" : { "name" : "beijing", "region" : "haidian" }}
    ```
    
    Json Path 为 `["$.city"]`。则匹配到的元素为：
    
    ```
    { "name" : "beijing", "region" : "haidian" }
    ```
    
    该元素会被转换为字符串进行后续导入操作：
    
    ```
    "{'name':'beijing','region':'haidian'}"
    ```

* 匹配失败

    当匹配失败时，将会返回 `null`。示例如下：
    
    Json 数据为：
    
    ```
    { "id": 123, "name" : "beijing"}
    ```
    
    Json Path 为 `["$.id", "$.info"]`。则匹配到的元素为 `123` 和 `null`。
    
    Doris 当前不区分 Json 数据中表示的 null 值，和匹配失败时产生的 null 值。假设 Json 数据为：
    
    ```
    { "id": 123, "name" : null }
    ```
    
    则使用以下两种 Json Path 会获得相同的结果：`123` 和 `null`。
    
    ```
    ["$.id", "$.name"]
    ```
    ```
    ["$.id", "$.info"]
    ```
    
* 完全匹配失败

    为防止一些参数设置错误导致的误操作。Doris 在尝试匹配一行数据时，如果所有列都匹配失败，则会认为这个是一个错误行。假设 Json 数据为：
    
    ```
    { "id": 123, "city" : "beijing" }
    ```
    
    如果 Json Path 错误的写为（或者不指定 Json Path 时，表中的列不包含 `id` 和 `city`）：
    
    ```
    ["$.ad", "$.infa"]
    ```
    
    则会导致完全匹配失败，则该行会标记为错误行，而不是产出 `null, null`。

## Json Path 和 Columns

Json Path 用于指定如何对 JSON 格式中的数据进行抽取，而 Columns 指定列的映射和转换关系。两者可以配合使用。

换句话说，相当于通过 Json Path，将一个 Json 格式的数据，按照 Json Path 中指定的列顺序进行了列的重排。之后，可以通过 Columns，将这个重排后的源数据和表的列进行映射。举例如下：

数据内容：

```
{"k1" : 1, "k2": 2}
```

表结构：

`k2 int, k1 int`

导入语句1（以 Stream Load 为例）：

```
curl -v --location-trusted -u root: -H "format: json" -H "jsonpaths: [\"$.k2\", \"$.k1\"]" -T example.json http://127.0.0.1:8030/api/db1/tbl1/_stream_load
```

导入语句1中，仅指定了 Json Path，没有指定 Columns。其中 Json Path 的作用是将 Json 数据按照 Json Path 中字段的顺序进行抽取，之后会按照表结构的顺序进行写入。最终导入的数据结果如下：

```
+------+------+
| k1   | k2   |
+------+------+
|    2 |    1 |
+------+------+
```

会看到，实际的 k1 列导入了 Json 数据中的 "k2" 列的值。这是因为，Json 中字段名称并不等同于表结构中字段的名称。我们需要显式的指定这两者之间的映射关系。

导入语句2：

```
curl -v --location-trusted -u root: -H "format: json" -H "jsonpaths: [\"$.k2\", \"$.k1\"]" -H "columns: k2, k1" -T example.json http://127.0.0.1:8030/api/db1/tbl1/_stream_load
```

相比如导入语句1，这里增加了 Columns 字段，用于描述列的映射关系，按 `k2, k1` 的顺序。即按Json Path 中字段的顺序抽取后，指定第一列为表中 k2 列的值，而第二列为表中 k1 列的值。最终导入的数据结果如下：

```
+------+------+
| k1   | k2   |
+------+------+
|    1 |    2 |
+------+------+
```

当然，如其他导入一样，可以在 Columns 中进行列的转换操作。示例如下：

```
curl -v --location-trusted -u root: -H "format: json" -H "jsonpaths: [\"$.k2\", \"$.k1\"]" -H "columns: k2, tmp_k1, k1 = tmp_k1 * 100" -T example.json http://127.0.0.1:8030/api/db1/tbl1/_stream_load
```

上述示例会将 k1 的值乘以 100 后导入。最终导入的数据结果如下：

```
+------+------+
| k1   | k2   |
+------+------+
|  100 |    2 |
+------+------+
```

## NULL 和 Default 值

示例数据如下：

```
[
    {"k1": 1, "k2": "a"},
    {"k1": 2},
    {"k1": 3, "k2": "c"},
]
```

表结构为：`k1 int null, k2 varchar(32) null default "x"`

导入语句如下：

```
curl -v --location-trusted -u root: -H "format: json" -H "strip_outer_array: true" -T example.json http://127.0.0.1:8030/api/db1/tbl1/_stream_load
```

用户可能期望的导入结果如下，即对于缺失的列，填写默认值。

```
+------+------+
| k1   | k2   |
+------+------+
|    1 |    a |
+------+------+
|    2 |    x |
+------+------+
|    3 |    c |
+------+------+
```

但实际的导入结果如下，即对于缺失的列，补上了 NULL。

```
+------+------+
| k1   | k2   |
+------+------+
|    1 |    a |
+------+------+
|    2 | NULL |
+------+------+
|    3 |    c |
+------+------+
```

这是因为通过导入语句中的信息，Doris 并不知道 “缺失的列是表中的 k2 列”。
如果要对以上数据按照期望结果导入，则导入语句如下：

```
curl -v --location-trusted -u root: -H "format: json" -H "strip_outer_array: true" -H "jsonpaths: [\"$.k1\", \"$.k2\"]" -H "columns: k1, tmp_k2, k2 = ifnull(tmp_k2, 'x')" -T example.json http://127.0.0.1:8030/api/db1/tbl1/_stream_load
```

## LargetInt与Decimal

Doris支持LargeInt与Decimal等数据范围更大，数据精度更高的数据类型。但是由于Doris使用的Rapid Json库对于数字类型能够解析的最大范围为Int64与Double，这导致了通过Json导入LargeInt或Decimal时可能会出现：精度丢失，数据转换出错等问题。

示例数据如下：

```
[
    {"k1": 1, "k2":9999999999999.999999 }
]
```


导入k2列类型为`Decimal(16, 9)`，数据为:`9999999999999.999999`。在进行Json导入时，由于Double转换的精度丢失导致了导入的数据为：`10000000000000.0002`，引发了导入出错。

为了解决这个问题，Doris在导入时提供了 `num_as_string`的开关。Doris在解析Json数据时会将数字类型转为字符串，然后在确保不会出现精度丢失的情况下进行导入。

```
curl -v --location-trusted -u root: -H "format: json" -H "num_as_string: true" -T example.json http://127.0.0.1:8030/api/db1/tbl1/_stream_load
```

但是开启这个开关会引起一些意想不到的副作用。Doris 当前暂不支持复合类型，如 Array、Map 等。所以当匹配到一个非基本类型时，Doris 会将该类型转换为 Json 格式的字符串，而`num_as_string`会同样将复合类型的数字转换为字符串，举个例子：
    
Json 数据为：

    { "id": 123, "city" : { "name" : "beijing", "city_id" : 1 }}

不开启`num_as_string`时，导入的city列的数据为:

`{ "name" : "beijing", "city_id" : 1 }`

而开启了`num_as_string`时，导入的city列的数据为:

`{ "name" : "beijing", "city_id" : "1" }`

注意，这里导致了复合类型原先为1的数字类型的city_id被作为字符串列处理并添加上了引号，与原始数据相比，产生了变化。

所以用在使用Json导入时，要尽量避免LargeInt与Decimal与复合类型的同时导入。如果无法避免，则需要充分了解开启`num_as_string`后对复合类型导入的**副作用**。


## 应用示例

### Stream Load

因为 Json 格式的不可拆分特性，所以在使用 Stream Load 导入 Json 格式的文件时，文件内容会被全部加载到内存后，才开始处理。因此，如果文件过大的话，可能会占用较多的内存。

假设表结构为：

```
id      INT     NOT NULL,
city    VARHCAR NULL,
code    INT     NULL
```

1. 导入单行数据1

    ```
    {"id": 100, "city": "beijing", "code" : 1}
    ```

    * 不指定 Json Path
    
        ```
        curl --location-trusted -u user:passwd -H "format: json" -T data.json http://localhost:8030/api/db1/tbl1/_stream_load
        ```
        
        导入结果：
        
        ```
        100     beijing     1
        ```
        
    * 指定 Json Path

        ```
        curl --location-trusted -u user:passwd -H "format: json" -H "jsonpaths: [\"$.id\",\"$.city\",\"$.code\"]" -T data.json http://localhost:8030/api/db1/tbl1/_stream_load
        ```
        
        导入结果：
        
        ```
        100     beijing     1
        ```
    
2. 导入单行数据2

    ```
    {"id": 100, "content": {"city": "beijing", "code" : 1}}
    ```

    * 指定 Json Path

        ```
        curl --location-trusted -u user:passwd -H "format: json" -H "jsonpaths: [\"$.id\",\"$.content.city\",\"$.content.code\"]" -T data.json http://localhost:8030/api/db1/tbl1/_stream_load
        ```

        导入结果：
        
        ```
        100     beijing     1
        ```

3. 导入多行数据

    ```
    [
        {"id": 100, "city": "beijing", "code" : 1},
        {"id": 101, "city": "shanghai"},
        {"id": 102, "city": "tianjin", "code" : 3},
        {"id": 103, "city": "chongqing", "code" : 4},
        {"id": 104, "city": ["zhejiang", "guangzhou"], "code" : 5},
        {
            "id": 105,
            "city": {
                "order1": ["guangzhou"]
            }, 
            "code" : 6
        }
    ]
    ```

    * 指定 Json Path

        ```
        curl --location-trusted -u user:passwd -H "format: json" -H "jsonpaths: [\"$.id\",\"$.city\",\"$.code\"]" -H "strip_outer_array: true" -T data.json http://localhost:8030/api/db1/tbl1/_stream_load
        ```

        导入结果：
        
        ```
        100     beijing                     1
        101     shanghai                    NULL
        102     tianjin                     3
        103     chongqing                   4
        104     ["zhejiang","guangzhou"]    5
        105     {"order1":["guangzhou"]}    6
        ```
    
4. 对导入数据进行转换

    数据依然是示例3中的多行数据，现需要对导入数据中的 `code` 列加1后导入。
    
    ```
    curl --location-trusted -u user:passwd -H "format: json" -H "jsonpaths: [\"$.id\",\"$.city\",\"$.code\"]" -H "strip_outer_array: true" -H "columns: id, city, tmpc, code=tmpc+1" -T data.json http://localhost:8030/api/db1/tbl1/_stream_load
    ```

    导入结果：
        
    ```
    100     beijing                     2
    101     shanghai                    NULL
    102     tianjin                     4
    103     chongqing                   5
    104     ["zhejiang","guangzhou"]    6
    105     {"order1":["guangzhou"]}    7
    ```

### Routine Load

Routine Load 对 Json 数据的处理原理和 Stream Load 相同。在此不再赘述。

对于 Kafka 数据源，每个 Massage 中的内容被视作一个完整的 Json 数据。如果一个 Massage 中是以 Array 格式的表示的多行数据，则会导入多行，而 Kafka 的 offset 只会增加 1。而如果一个 Array 格式的 Json 表示多行数据，但是因为 Json 格式错误导致解析 Json 失败，则错误行只会增加 1（因为解析失败，实际上 Doris 无法判断其中包含多少行数据，只能按一行错误数据记录）。
