---
{
    "title": "http",
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

## Http

### Name

### Description

The HTTP table function (table-valued-function,tvf) allows users to upload data using SQL

#### syntax
```sql
curl --location-trusted -u user:passwd -H "sql: '${load_sql}'" -T data.csv http://127.0.0.1:8030/api/v2/_load
```

Where ${load_sql} is in the following format
```
insert into db.table select * from 
http(
"format" = "CSV", 
"column_separator" = ","
...
) [where t1 > 0];
```

**parameter description**

Related parameters for accessing http:
- column_separator

  Used to specify the column separator in the load file. The default is `\t`. If it is an invisible character, you need to add `\x` as a prefix and hexadecimal to indicate the separator.

  For example, the separator `\x01` of the hive file needs to be specified as `-H "column_separator:\x01"`.

  You can use a combination of multiple characters as the column separator.

- line_delimiter

  Used to specify the line delimiter in the load file. The default is `\n`.

  You can use a combination of multiple characters as the column separator.

- max_filter_ratio

  The maximum tolerance rate of the import task is 0 by default, and the range of values is 0-1. When the import error rate exceeds this value, the import fails.

  If the user wishes to ignore the wrong row, the import can be successful by setting this parameter greater than 0.

  The calculation formula is as follows:

  ``` (dpp.abnorm.ALL / (dpp.abnorm.ALL + dpp.norm.ALL ) ) > max_filter_ratio ```

  ``` dpp.abnorm.ALL``` denotes the number of rows whose data quality is not up to standard. Such as type mismatch, column mismatch, length mismatch and so on.

  ``` dpp.norm.ALL ``` refers to the number of correct data in the import process. The correct amount of data for the import task can be queried by the ``SHOW LOAD` command.

The number of rows in the original file = `dpp.abnorm.ALL + dpp.norm.ALL`

- Partitions

  Partitions information for tables to be imported will not be imported if the data to be imported does not belong to the specified Partition. These data will be included in `dpp.abnorm.ALL`.

- format

  Specify the import data format, support csv, json, the default is csv


- exec_mem_limit

  Memory limit. Default is 2GB. Unit is Bytes

### Examples

上传数据
```shell
curl -v --location-trusted -u root: -H "sql: insert into test.t1(k1,k2) select k1,k2 from http(\"format\" = \"CSV\", \"column_separator\" = \",\")" -T example.csv http://127.0.0.1:8030/api/v2/_load
```


### Keywords

    http, table-valued-function, tvf
