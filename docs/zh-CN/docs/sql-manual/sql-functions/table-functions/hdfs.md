---
{
    "title": "hdfs",
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

# `HDFS`

## description

HDFS表函数（table-valued-function,tvf），可以读取hdfs上的文件，将文件中的内容转换为doris中的内存表,生成一张doris临时表。目前支持`csv/csv_with_names/csv_with_names_and_types/json/parquet/orc`文件格式。

## 语法

```
hdfs(
  "uri" = "..",
  "fs.defaultFS" = "...",
  "hadoop.username" = "...",
  "format" = "csv",
  "keyn" = "valuen" 
  ...
  );
```

## 参数说明

访问hdfs相关参数：
- `uri`：（必填） 访问S3的uri，S3表函数会根据 `use_path_style` 参数来决定是否使用 path style 访问方式，默认为 virtual-hosted style 方式
- `fs.defaultFS`：（必填）
- `hadoop.username`： （必填）可以是任意字符串，但不能为空
- `hadoop.security.authentication`：（选填）
- `hadoop.username`：（选填）
- `hadoop.kerberos.principal`：（选填）
- `hadoop.kerberos.keytab`：（选填）
- `dfs.client.read.shortcircuit`：（选填）
- `dfs.domain.socket.path`：（选填）

文件格式相关参数
- `format`：(必填) 目前支持 `csv/csv_with_names/csv_with_names_and_types/json/parquet/orc`
- `column_separator`：(选填) 列分割符, 默认为`,`。 
- `line_delimiter`：(选填) 行分割符，默认为`\n`。

    下面6个参数是用于json格式的导入，具体使用方法可以参照：[Json Load](../../../data-operate/import/import-way/load-json-format.md)

- `read_json_by_line`： (选填) 默认为 `"true"`
- `strip_outer_array`： (选填) 默认为 `"false"`
- `json_root`： (选填) 默认为空
- `json_paths`： (选填) 默认为空
- `num_as_string`： (选填) 默认为 `false`
- `fuzzy_parse`： (选填) 默认为 `false`

## 使用说明

关于HDFS tvf的详细使用方法可以参照 [S3](./s3.md) tvf, 唯一的不同的是访问存储系统的方式不一样。

### keywords

    hdfs, table-valued-function, tvf