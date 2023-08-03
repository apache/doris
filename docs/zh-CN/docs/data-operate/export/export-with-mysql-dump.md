---
{
    "title": "MYSQLDUMP 导出表结构或数据",
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

# 使用 MYSQLDUMP 数据导出表结构或者数据
Doris 在0.15 之后的版本已经支持通过`mysqldump` 工具导出数据或者表结构

## 使用示例
### 导出
1. 导出 test 数据库中的 table1 表：`mysqldump -h127.0.0.1 -P9030 -uroot --no-tablespaces --databases test --tables table1`
2. 导出 test 数据库中的 table1 表结构：`mysqldump -h127.0.0.1 -P9030 -uroot --no-tablespaces --databases test --tables table1 --no-data`
3. 导出 test1, test2 数据库中所有表：`mysqldump -h127.0.0.1 -P9030 -uroot --no-tablespaces --databases test1 test2`
4. 导出所有数据库和表 `mysqldump -h127.0.0.1 -P9030 -uroot --no-tablespaces --all-databases`
   更多的使用参数可以参考`mysqldump` 的使用手册
### 导入
`mysqldump` 导出的结果可以重定向到文件中，之后可以通过 source 命令导入到Doris 中 `source filename.sql`
## 注意
1. 由于Doris  中没有mysql 里的 tablespace 概念，因此在使用mysqldump 时要加上 `--no-tablespaces` 参数
2. 使用mysqldump 导出数据和表结构仅用于开发测试或者数据量很小的情况，请勿用于大数据量的生产环境
