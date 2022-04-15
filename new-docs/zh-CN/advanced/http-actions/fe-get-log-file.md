---
{
    "title": "get\\_log\\_file",
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

# get\_log\_file

用户可以通过该 HTTP 接口获取 FE 的日志文件。

## 日志类型

支持获取以下类型的 FE 日志：

1. fe.audit.log（审计日志）

    审计日志记录了对应 FE 节点的所有请求语句已经请求的信息。审计日志的文件命名规则如下：

    ```
    fe.audit.log                # 当前的最新日志
    fe.audit.log.20190603.1     # 对应日期的审计日志，当对应日期的日志大小超过 1GB 后，会生成序号后缀。序号越小的日志，内容越新。
    fe.audit.log.20190603.2
    fe.audit.log.20190602.1
    ...
    ```

## 接口示例

1. 获取对应类型的日志文件列表

    示例：
    
    `curl -v -X HEAD -uuser:passwd http://fe_host:http_port/api/get_log_file?type=fe.audit.log`
    
    返回结果：
    
    ```
    HTTP/1.1 200 OK
    file_infos: {"fe.audit.log":24759,"fe.audit.log.20190528.1":132934}
    content-type: text/html
    connection: keep-alive
    ```
    
    在返回的 header 中，`file_infos` 字段以 json 格式展示文件列表以及对应文件大小（单位字节）
    
2. 下载日志文件

    示例：
    
    ```
    curl -X GET -uuser:passwd http://fe_host:http_port/api/get_log_file?type=fe.audit.log\&file=fe.audit.log.20190528.1
    ```
    
    返回结果：
    
    以文件的形式下载指定的文件。

## 接口说明

该接口需要 admin 权限。
