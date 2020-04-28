---
{
    "title": "Configuration",
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

# Configuration

## brpc_max_body_size

  This configuration is mainly used to modify the parameter max_body_size of brpc. The default configuration is 64M. It usually occurs in multi distinct + no group by + exceeds 1t data. In particular, if you find that the query is stuck, and be appears the word "body size is too large" in log.

  Because this is a brpc configuration, users can also directly modify this parameter on-the-fly by visiting ```http://host:brpc_port/flags```

## max_running_txn_num_per_db

   This configuration is mainly used to control the number of concurrent load job in the same db. The default configuration is 100. When the number of concurrent load job exceeds the configured value, the load which is synchronously executed will fail, such as stream load. The load which is asynchronously will always be in a pending state such as broker load.

   It is generally not recommended to change this property. If the current load concurrency exceeds this value, you need to first check if a single load job is too slow, or if there are too many small files, there is no problem of load after merging those small files.

   Error information such as: current running txns on db xxx is xx, larger than limit xx. The above info is related by this property.
