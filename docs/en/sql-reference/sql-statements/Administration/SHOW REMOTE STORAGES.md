---
{
    "title": "SHOW REMOTE STORAGES",
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

# SHOW REMOTE STORAGES

## Description

    This statement is used to view the existing remote storages
    Grammar:
        SHOW REMOTE STORAGES;

    Explain:
        1. Name: name of remote storage
        2. Type: type of remote storage
        3. Properties: properties of reomte storage

## Example

    View the remote storage information of the current cluster.

    ```
    mysql> show remote storages;
    +-----------+------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | Name      | Type | Properties                                                                                                                                                                                                                                        |
    +-----------+------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | remote_s3 | S3   | "s3_secret_key"  =  "*XXX",
    "s3_region"  =  "bj",
    "s3_access_key"  =  "bbb",
    "s3_max_connections"  =  "50",
    "s3_connection_timeout_ms"  =  "1000",
    "s3_root_path"  =  "/path/to/root",
    "s3_endpoint"  =  "bj",
    "s3_request_timeout_ms"  =  "3000" |
    +-----------+------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    ```

## keyword

    SHOW, REMOTE, REMOTE STORAGES
