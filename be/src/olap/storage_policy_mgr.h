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

#include <stdint.h>

#include <mutex>
#include <string>
#include <unordered_map>

#include "common/status.h"

struct StoragePolicy {
    std::string storage_policy_name;
    int64_t cooldown_datetime;
    int64_t cooldown_ttl;
    // s3 resource
    std::string s3_endpoint;
    std::string s3_region;
    std::string s3_ak;
    std::string s3_sk;
    std::string root_path;
    std::string bucket;
    std::string md5_sum;
    int64_t s3_conn_timeout_ms;
    int64_t s3_max_conn;
    int64_t s3_request_timeout_ms;
};

inline std::ostream& operator<<(std::ostream& out, const StoragePolicy& m) {
    out << "storage_policy_name: " << m.storage_policy_name
        << " cooldown_datetime: " << m.cooldown_datetime << " cooldown_ttl: " << m.cooldown_ttl
        << " s3_endpoint: " << m.s3_endpoint << " s3_region: " << m.s3_region
        << " root_path: " << m.root_path << " bucket: " << m.bucket << " md5_sum: " << m.md5_sum
        << " s3_conn_timeout_ms: " << m.s3_conn_timeout_ms << " s3_max_conn: " << m.s3_max_conn
        << " s3_request_timeout_ms: " << m.s3_request_timeout_ms;
    return out;
}

namespace doris {
class ExecEnv;

class StoragePolicyMgr {
public:
    using StoragePolicyPtr = std::shared_ptr<StoragePolicy>;
    StoragePolicyMgr() = default;

    ~StoragePolicyMgr() = default;

    // fe push update policy to be
    void update(const std::string& name, const StoragePolicyPtr& policy);

    // periodic pull from fe
    void periodic_put(const std::string& name, const StoragePolicyPtr& policy);

    StoragePolicyPtr get(const std::string& name);

    void del(const std::string& name);

private:
    std::mutex _mutex;
    std::unordered_map<std::string, StoragePolicyPtr> _policy_map;
};
} // namespace doris
