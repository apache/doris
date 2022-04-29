---
{
    "title": "get\\_log\\_file",
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

# get\_log\_file

To get FE log via HTTP

## Types of FE log

1. fe.audit.log (Audit log)

    The audit log records the all statements executed. Audit log's name format as follow:

    ```
    fe.audit.log                # The latest audit log
    fe.audit.log.20190603.1     # The historical audit log. The smaller the sequence number, the newer the log.
    fe.audit.log.20190603.2
    fe.audit.log.20190602.1
    ...
    ```

## Example

1. Get the list of specified type of logs

    Example
    
    `curl -v -X HEAD -uuser:passwd http://fe_host:http_port/api/get_log_file?type=fe.audit.log`
    
    Returns:
    
    ```
    HTTP/1.1 200 OK
    file_infos: {"fe.audit.log":24759,"fe.audit.log.20190528.1":132934}
    content-type: text/html
    connection: keep-alive
    ```
    
    In the header of result, the `file_infos` section saves the file list and file size in JSON format.
    
2. Download files

    Example:
    
    ```
    curl -X GET -uuser:passwd http://fe_host:http_port/api/get_log_file?type=fe.audit.log\&file=fe.audit.log.20190528.1
    ```

## Notification

Need ADMIN privilege.
