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

class RemoteEnvMgr {
public:
    RemoteEnvMgr() {}
    ~RemoteEnvMgr() {}

    Status init(const std::string& storage_name_dir);

    std::shared_ptr<RemoteEnv> get_remote_env(const std::string& storage_name);

    Status create_remote_storage(const StorageParamPB& storage_param);

    Status get_storage_param(const std::string& storage_name, StorageParamPB* storage_param);

    Status get_root_path(const std::string& storage_name, std::string* root_path);

private:
    Status _create_remote_storage_internal(const StorageParamPB& storage_param);
    Status _check_exist(const StorageParamPB& storage_param_pb);
    Status _serialize(const StorageParamPB& storage_param_pb, std::string* meta_binary);
    Status _deserialize(const std::string& meta_binary, StorageParamPB* storage_param_pb);

    RWMutex _remote_env_lock;
    std::map<std::string, time_t> _remote_env_active_time;
    std::map<std::string, std::shared_ptr<RemoteEnv>> _remote_env_map;
    std::map<std::string, StorageParamPB> _storage_param_map;
    std::string _storage_param_dir;
    bool _is_inited = false;
};

} // namespace doris