---
{
    "title": "CHECK/RESET Stub Cache",
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

# CHECK/RESET Stub Cache
## description
   
### Check Stub Cache
    Check whether the connection cache is available

    Description: Check whether the connection cache is available, the maximum load is 10M
    METHOD: GET
    URI: http://be_host:be_http_port/api/check_rpc_channel/{host_to_check}/{remot_brpc_port}/{payload_size}
   
### Reset Stub Cache
    This api is used to reset the connection cache of brpc. Endpoints can be in the form of `all` to clear all caches, `host1:port1,host2:port2,...`: clear to the cache of the specified target

    Description: Reset connection cache
    METHOD: GET
    URI: http://be_host:be_http_port/api/reset_rpc_channel/{endpoints}
## example

    curl -X GET "http://host:port/api/check_rpc_channel/host2/8060/1024000"
    curl -X GET "http://host:port/api/reset_rpc_channel/all"

