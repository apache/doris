---
{
    "title": "DROP FILE",
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

# DROP FILE
## description

    该语句用于删除一个已上传的文件。

    语法：

        DROP FILE "file_name" [FROM database]
        [properties]

    说明：
        file_name:  文件名。
        database: 文件归属的某一个 db，如果没有指定，则使用当前 session 的 db。
        properties 支持以下参数:

            catalog: 必须。文件所属分类。

## example

    1. 删除文件 ca.pem

        DROP FILE "ca.pem" properties("catalog" = "kafka");

## keyword
    DROP,FILE
