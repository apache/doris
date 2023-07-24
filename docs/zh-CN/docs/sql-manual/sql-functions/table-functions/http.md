---
{
    "title": "http",
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

## Http

### Name

### Description

http 表函数（table-valued-function,tvf），可以让用户使用 sql 方式上传数据

#### syntax
```sql
curl --location-trusted -u user:passwd -H "sql: '${load_sql}'" -T data.csv http://127.0.0.1:8030/api/v2/_load
```

其中 ${load_sql} 为以下格式
```
insert into db.table select * from 
http(
"format" = "CSV", 
"column_separator" = ","
...
) [where t1 > 0];
```

**参数说明**

访问 http 相关参数：
- column_separator

  用于指定导入文件中的列分隔符，默认为\t。如果是不可见字符，则需要加\x作为前缀，使用十六进制来表示分隔符。

  如 hive 文件的分隔符\x01，需要指定为-H "column_separator:\x01"。

  可以使用多个字符的组合作为列分隔符。

- line_delimiter

  用于指定导入文件中的换行符，默认为\n。

  可以使用做多个字符的组合作为换行符。

- max_filter_ratio

  导入任务的最大容忍率，默认为0容忍，取值范围是0~1。当导入的错误率超过该值，则导入失败。

  如果用户希望忽略错误的行，可以通过设置这个参数大于 0，来保证导入可以成功。

  计算公式为：

  `(dpp.abnorm.ALL / (dpp.abnorm.ALL + dpp.norm.ALL ) ) > max_filter_ratio`

  `dpp.abnorm.ALL` 表示数据质量不合格的行数。如类型不匹配，列数不匹配，长度不匹配等等。

  `dpp.norm.ALL` 指的是导入过程中正确数据的条数。可以通过 `SHOW LOAD` 命令查询导入任务的正确数据量。

  原始文件的行数 = `dpp.abnorm.ALL + dpp.norm.ALL`

- Partitions

  待导入表的 Partition 信息，如果待导入数据不属于指定的 Partition 则不会被导入。这些数据将计入 `dpp.abnorm.ALL`

- format

  指定导入数据格式，支持csv、json，默认是csv


- exec_mem_limit

  导入内存限制。默认为 2GB，单位为字节。

### Examples

上传数据
```shell
curl -v --location-trusted -u root: -H "sql: insert into test.t1(k1,k2) select k1,k2 from http(\"format\" = \"CSV\", \"column_separator\" = \",\")" -T example.csv http://127.0.0.1:8030/api/v2/_load
```


### Keywords

    http, table-valued-function, tvf
