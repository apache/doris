---
{
    "title": "local",
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

## Local

### Name

<version since="dev">

local

</version>

### Description

Local table-valued-function(tvf), allows users to read and access local file contents on be node, just like accessing relational table. Currently supports `csv/csv_with_names/csv_with_names_and_types/json/parquet/orc` file format.

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

**parameter description**

Related parameters for accessing local file on be node:

- `file_path`: (required) The path of the file to be read, which is a relative path to the `user_files_secure_path` directory, where `user_files_secure_path` parameter [can be configured on be](../../../admin-manual/config/be-config.md).
- `backend_id`: (required) The backend id where the file resides. The `backend_id` can be obtained by `show backends` command.

File format parameters:

- `format`: (required) Currently support `csv/csv_with_names/csv_with_names_and_types/json/parquet/orc`
- `column_separator`: (optional) default `,`.
- `line_delimiter`: (optional) default `\n`.
- `compress_type`: (optional) Currently support `UNKNOWN/PLAIN/GZ/LZO/BZ2/LZ4FRAME/DEFLATE`. Default value is `UNKNOWN`, it will automatically infer the type based on the suffix of `uri`.

    The following 6 parameters are used for loading in json format. For specific usage methods, please refer to: [Json Load](../../../data-operate/import/import-way/load-json-format.md)

- `read_json_by_line`: (optional) default `"true"`
- `strip_outer_array`: (optional) default `"false"`
- `json_root`: (optional) default `""`
- `json_paths`: (optional) default `""`
- `num_as_string`: (optional) default `false`
- `fuzzy_parse`: (optional) default `false`

    <version since="dev">The following 2 parameters are used for loading in csv format</version>

- `trim_double_quotes`: Boolean type (optional), the default value is `false`. True means that the outermost double quotes of each field in the csv file are trimmed.
- `skip_lines`: Integer type (optional), the default value is 0. It will skip some lines in the head of csv file. It will be disabled when the format is `csv_with_names` or `csv_with_names_and_types`.

### Examples

Read and access csv format files located at path `${DORIS_HOME}/student.csv`:

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

Can be used with `desc function` :

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

  For more detailed usage of local tvf, please refer to [S3](./s3.md) tvf, The only difference between them is the way of accessing the storage system.
