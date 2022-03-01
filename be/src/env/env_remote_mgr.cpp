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

#include "common/config.h"
#include "env/env_remote_mgr.h"

namespace doris {

std::shared_ptr<RemoteEnv> RemoteEnvMgr::get_remote_env(const TStorageParam& storage_param) {
    std::string key = "";
    switch (storage_param.storage_medium) {
        case TStorageMedium::S3:
        default:
            const TS3StorageParam& s3_storage_param = storage_param.s3_storage_param;
            if (s3_storage_param.s3_endpoint.empty()) {
                return nullptr;
            }
            key = s3_storage_param.s3_endpoint;
    }
    WriteLock wrlock(&_remote_env_lock);
    if (_remote_env_map.find(key) != _remote_env_map.end()) {
        _remote_env_active_time[key] = time(nullptr);
        return _remote_env_map[key];
    }

    if (_remote_env_map.size() >= doris::config::max_remote_storage_count) {
        std::map<std::string, time_t>::iterator itr = _remote_env_active_time.begin();
        std::string timeout_key = itr->first;
        time_t min_active_time = itr->second;
        ++itr;
        for (; itr != _remote_env_active_time.end(); ++itr) {
            if (itr->second < min_active_time) {
                timeout_key = itr->first;
                min_active_time = itr->second;
            }
        }
        _remote_env_map.erase(key);
        _remote_env_active_time.erase(key);
    }
    std::shared_ptr<RemoteEnv> remote_env(new RemoteEnv());
    if (!remote_env->init_conf(storage_param).ok()) {
        return nullptr;
    }
    _remote_env_map[key] = remote_env;
    _remote_env_active_time[key] = time(nullptr);
    return remote_env;
}

} // namespace doris