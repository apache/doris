---
{
    "title": "local",
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

## local

### Name

<version since="dev">

local

</version>

### Description

Local表函数（table-valued-function,tvf），可以让用户像访问关系表格式数据一样，读取并访问 be 上的文件内容。目前支持`csv/csv_with_names/csv_with_names_and_types/json/parquet/orc`文件格式。

#### syntax
```sql
local(
  "file_path" = "path/to/file.txt", 
  "backend_id" = "be_id",
  "format" = "csv",
  "keyn" = "valuen" 
  ...
  );
```

**参数说明**

访问local文件的相关参数：
- `file_path`：（必填）待读取文件的路径，该路径是一个相对于 `user_files_secure_path` 目录的相对路径, 其中 `user_files_secure_path` 参数是 [be的一个配置项](../../../admin-manual/config/be-config.md) 。
- `backend_id`: （必填）文件所在的 be id。 `backend_id` 可以通过 `show backends` 命令得到。

文件格式相关参数
- `format`：(必填) 目前支持 `csv/csv_with_names/csv_with_names_and_types/json/parquet/orc`
- `column_separator`：(选填) 列分割符, 默认为`,`。 
- `line_delimiter`：(选填) 行分割符，默认为`\n`。
- `compress_type`: (选填) 目前支持 `UNKNOWN/PLAIN/GZ/LZO/BZ2/LZ4FRAME/DEFLATE`。 默认值为 `UNKNOWN`, 将会根据 `uri` 的后缀自动推断类型。

    下面6个参数是用于json格式的导入，具体使用方法可以参照：[Json Load](../../../data-operate/import/import-way/load-json-format.md)

- `read_json_by_line`： (选填) 默认为 `"true"`
- `strip_outer_array`： (选填) 默认为 `"false"`
- `json_root`： (选填) 默认为空
- `json_paths`： (选填) 默认为空
- `num_as_string`： (选填) 默认为 `false`
- `fuzzy_parse`： (选填) 默认为 `false`

    <version since="dev">下面2个参数是用于csv格式的导入</version>

- `trim_double_quotes`： 布尔类型，选填，默认值为 `false`，为 `true` 时表示裁剪掉 csv 文件每个字段最外层的双引号
- `skip_lines`： 整数类型，选填，默认值为0，含义为跳过csv文件的前几行。当设置format设置为 `csv_with_names` 或 `csv_with_names_and_types` 时，该参数会失效 

### Examples

读取和访问位于路径`${DORIS_HOME}/student.csv`的 csv格式文件：

```sql
mysql> select * from local(
      "file_path" = "student.csv", 
      "backend_id" = "10003", 
      "format" = "csv");
+------+---------+--------+
| c1   | c2      | c3     |
+------+---------+--------+
| 1    | alice   | 18     |
| 2    | bob     | 20     |
| 3    | jack    | 24     |
| 4    | jackson | 19     |
| 5    | liming  | d18    |
+------+---------+--------+
```

可以配合`desc function`使用

```sql
mysql> desc function local(
      "file_path" = "student.csv", 
      "backend_id" = "10003", 
      "format" = "csv");
+-------+------+------+-------+---------+-------+
| Field | Type | Null | Key   | Default | Extra |
+-------+------+------+-------+---------+-------+
| c1    | TEXT | Yes  | false | NULL    | NONE  |
| c2    | TEXT | Yes  | false | NULL    | NONE  |
| c3    | TEXT | Yes  | false | NULL    | NONE  |
+-------+------+------+-------+---------+-------+
```

### Keywords

    local, table-valued-function, tvf

### Best Practice

  关于local tvf的更详细使用方法可以参照 [S3](./s3.md) tvf, 唯一不同的是访问存储系统的方式不一样。
