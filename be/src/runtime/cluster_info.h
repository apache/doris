// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <gen_cpp/Types_types.h>

#include <string>

namespace doris {

// This class is used to save the cluster info
// like cluster id, epoch, cloud_unique_id, etc.
// These info are usually in heartbeat from Master FE.
class ClusterInfo {
public:
    // Unique cluster id
    int32_t cluster_id = 0;
    // Master FE addr: ip:rpc_port
    TNetworkAddress master_fe_addr;
    // Master FE http_port
    int32_t master_fe_http_port = 0;
    // Unique cluster token
    std::string token = "";
    // Backend ID
    int64_t backend_id = 0;

    // Auth token for internal authentication
    // Save the last 2 tokens to avoid token invalid during token update
    std::string curr_auth_token = "";
    std::string last_auth_token = "";
};

} // namespace doris
