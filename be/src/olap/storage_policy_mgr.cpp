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

#include "olap/storage_policy_mgr.h"

#include "io/fs/file_system.h"
#include "io/fs/file_system_map.h"
#include "io/fs/s3_file_system.h"
#include "util/s3_util.h"

namespace doris {

void StoragePolicyMgr::update(const std::string& name, StoragePolicyPtr policy) {
    std::lock_guard<std::mutex> l(_mutex);
    auto it = _policy_map.find(name);
    if (it != _policy_map.end()) {
        // just support change ak, sk, cooldown_ttl, cooldown_datetime
        LOG(INFO) << "update storage policy name: " << name;
        auto s3_fs = std::dynamic_pointer_cast<io::S3FileSystem>(
                io::FileSystemMap::instance()->get(name));
        DCHECK(s3_fs);
        s3_fs->set_ak(policy->s3_ak);
        s3_fs->set_sk(policy->s3_sk);
        s3_fs->connect();
        it->second = std::move(policy);
    } else {
        // can't find name's policy, so do nothing.
    }
}

void StoragePolicyMgr::periodic_put(const std::string& name, StoragePolicyPtr policy) {
    std::lock_guard<std::mutex> l(_mutex);
    auto it = _policy_map.find(name);
    if (it == _policy_map.end()) {
        LOG(INFO) << "add storage policy name: " << name << " to map";
        std::map<std::string, std::string> aws_properties = {
                {S3_AK, policy->s3_ak},
                {S3_SK, policy->s3_sk},
                {S3_ENDPOINT, policy->s3_endpoint},
                {S3_REGION, policy->s3_region},
                {S3_MAX_CONN_SIZE, std::to_string(policy->s3_max_conn)},
                {S3_REQUEST_TIMEOUT_MS, std::to_string(policy->s3_request_timeout_ms)},
                {S3_CONN_TIMEOUT_MS, std::to_string(policy->s3_conn_timeout_ms)}};
        auto s3_fs = std::make_shared<io::S3FileSystem>(aws_properties, policy->bucket,
                                                        policy->root_path, name);
        s3_fs->connect();
        io::FileSystemMap::instance()->insert(name, std::move(s3_fs));
        _policy_map.emplace(name, std::move(policy));
    } else if (it->second->md5_sum != policy->md5_sum) {
        // fe change policy
        // just support change ak, sk, cooldown_ttl, cooldown_datetime
        LOG(INFO) << "update storage policy name: " << name;
        auto s3_fs = std::dynamic_pointer_cast<io::S3FileSystem>(
                io::FileSystemMap::instance()->get(name));
        DCHECK(s3_fs);
        s3_fs->set_ak(policy->s3_ak);
        s3_fs->set_sk(policy->s3_sk);
        s3_fs->connect();
        it->second = std::move(policy);
    }
}

StoragePolicyMgr::StoragePolicyPtr StoragePolicyMgr::get(const std::string& name) {
    std::lock_guard<std::mutex> l(_mutex);
    auto it = _policy_map.find(name);
    if (it != _policy_map.end()) {
        return it->second;
    }
    return nullptr;
}

void StoragePolicyMgr::del(const std::string& name) {
    std::lock_guard<std::mutex> l(_mutex);
    _policy_map.erase(name);
}

} // namespace doris
