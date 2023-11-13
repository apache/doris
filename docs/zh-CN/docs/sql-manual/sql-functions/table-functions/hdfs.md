---
{
    "title": "HDFS",
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

## HDFS

### Name

<version since="1.2">

hdfs

</version>

### Description

HDFS表函数（table-valued-function,tvf），可以让用户像访问关系表格式数据一样，读取并访问 HDFS 上的文件内容。目前支持`csv/csv_with_names/csv_with_names_and_types/json/parquet/orc`文件格式。

#### syntax
```sql
hdfs(
  "uri" = "..",
  "fs.defaultFS" = "...",
  "hadoop.username" = "...",
  "format" = "csv",
  "keyn" = "valuen" 
  ...
  );
```

**参数说明**

访问hdfs相关参数：
- `uri`：（必填） 访问hdfs的uri。如果uri路径不存在或文件都是空文件，hdfs tvf将返回空集合。
- `fs.defaultFS`：（必填）
- `hadoop.username`： （必填）可以是任意字符串，但不能为空
- `hadoop.security.authentication`：（选填）
- `hadoop.username`：（选填）
- `hadoop.kerberos.principal`：（选填）
- `hadoop.kerberos.keytab`：（选填）
- `dfs.client.read.shortcircuit`：（选填）
- `dfs.domain.socket.path`：（选填）

访问 HA 模式 HDFS 相关参数：
- `dfs.nameservices`：（选填）
- `dfs.ha.namenodes.your-nameservices`：（选填）
- `dfs.namenode.rpc-address.your-nameservices.your-namenode`：（选填）
- `dfs.client.failover.proxy.provider.your-nameservices`：（选填）

文件格式相关参数
- `format`：(必填) 目前支持 `csv/csv_with_names/csv_with_names_and_types/json/parquet/orc/avro`
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

其他参数：
- `path_partition_keys`：（选填）指定文件路径中携带的分区列名，例如/path/to/city=beijing/date="2023-07-09", 则填写`path_partition_keys="city,date"`，将会自动从路径中读取相应列名和列值进行导入。

### Examples

读取并访问 HDFS 存储上的csv格式文件
```sql
MySQL [(none)]> select * from hdfs(
            "uri" = "hdfs://127.0.0.1:842/user/doris/csv_format_test/student.csv",
            "fs.defaultFS" = "hdfs://127.0.0.1:8424",
            "hadoop.username" = "doris",
            "format" = "csv");
+------+---------+------+
| c1   | c2      | c3   |
+------+---------+------+
| 1    | alice   | 18   |
| 2    | bob     | 20   |
| 3    | jack    | 24   |
| 4    | jackson | 19   |
| 5    | liming  | 18   |
+------+---------+------+
```

读取并访问 HA 模式的 HDFS 存储上的csv格式文件
```sql
MySQL [(none)]> select * from hdfs(
            "uri" = "hdfs://127.0.0.1:842/user/doris/csv_format_test/student.csv",
            "fs.defaultFS" = "hdfs://127.0.0.1:8424",
            "hadoop.username" = "doris",
            "format" = "csv",
            "dfs.nameservices" = "my_hdfs",
            "dfs.ha.namenodes.my_hdfs" = "nn1,nn2",
            "dfs.namenode.rpc-address.my_hdfs.nn1" = "nanmenode01:8020",
            "dfs.namenode.rpc-address.my_hdfs.nn2" = "nanmenode02:8020",
            "dfs.client.failover.proxy.provider.my_hdfs" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
+------+---------+------+
| c1   | c2      | c3   |
+------+---------+------+
| 1    | alice   | 18   |
| 2    | bob     | 20   |
| 3    | jack    | 24   |
| 4    | jackson | 19   |
| 5    | liming  | 18   |
+------+---------+------+
```

可以配合`desc function`使用

```sql
MySQL [(none)]> desc function hdfs(
            "uri" = "hdfs://127.0.0.1:8424/user/doris/csv_format_test/student_with_names.csv",
            "fs.defaultFS" = "hdfs://127.0.0.1:8424",
            "hadoop.username" = "doris",
            "format" = "csv_with_names");
```

### Keywords

    hdfs, table-valued-function, tvf

### Best Practice

  关于HDFS tvf的更详细使用方法可以参照 [S3](./s3.md) tvf, 唯一不同的是访问存储系统的方式不一样。
