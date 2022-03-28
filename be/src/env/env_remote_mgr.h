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

#include <string>
#include <map>
#include <vector>

#include "env/env_remote.h"
#include "util/mutex.h"

namespace doris {

// RemoteEnvMgr is used to manage RemoteEnv, it has (key -> RemoteEnv) map used to connect remote storage
class RemoteEnvMgr {
public:
    RemoteEnvMgr() {}
    ~RemoteEnvMgr() {}

    // init() is called when be is started, storage_name_dir is the file path for remote parameter in local cache_path.
    Status init(const std::string& storage_name_dir);

    // get_remote_env by storage_name, one storage_name matches a remote storage_backend.
    std::shared_ptr<RemoteEnv> get_remote_env(const std::string& storage_name);

    // create a new remote storage_backend when it doesn't exist.
    Status create_remote_storage(const StorageParamPB& storage_param);

    // get storage_param by storage_name.
    Status get_storage_param(const std::string& storage_name, StorageParamPB* storage_param);

    // get root_path of remote storage by storage_name
    Status get_root_path(const std::string& storage_name, std::string* root_path);

    // get root_path of remote storage from storage_param
    static std::string get_root_path_from_param(const StorageParamPB& storage_param);
private:
    Status _create_remote_storage_internal(const StorageParamPB& storage_param);
    Status _check_exist(const StorageParamPB& storage_param_pb);
    Status _serialize_param(const StorageParamPB& storage_param_pb, std::string* meta_binary);
    Status _deserialize_param(const std::string& meta_binary, StorageParamPB* storage_param_pb);

    std::shared_mutex _remote_env_lock;
    std::map<std::string, time_t> _remote_env_active_time;
    // key is storage_name, value is RemoteEnv with one storage_backend.
    std::map<std::string, std::shared_ptr<RemoteEnv>> _remote_env_map;
    std::map<std::string, StorageParamPB> _storage_param_map;
    std::string _storage_param_dir;
    bool _is_inited = false;
};

} // namespace doris