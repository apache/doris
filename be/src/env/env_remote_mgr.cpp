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
#include "common/status.h"
#include "env/env_util.h"
#include "env/env_remote_mgr.h"
#include "gutil/strings/substitute.h"
#include "util/faststring.h"
#include "util/file_utils.h"

namespace doris {

Status RemoteEnvMgr::init(const std::string& storage_param_dir) {
    if (_is_inited) {
        return Status::OK();
    }
    Status exist_status = Env::Default()->path_exists(storage_param_dir);
    if (!exist_status.ok() &&
        (!exist_status.is_not_found() || !Env::Default()->create_dirs(storage_param_dir).ok())) {
        RETURN_NOT_OK_STATUS_WITH_WARN(Status::IOError(strings::Substitute(
                "failed to create remote storage_param root path $0", storage_param_dir)),
                                       "create_dirs failed");
    }

    std::vector<std::string> file_names;
    RETURN_IF_ERROR(FileUtils::list_files(Env::Default(), storage_param_dir, &file_names));
    for (auto& file_name : file_names) {
        faststring buf;
        RETURN_NOT_OK_STATUS_WITH_WARN(env_util::read_file_to_string(
                Env::Default(), storage_param_dir + "/" + file_name, &buf),
                                       strings::Substitute("load storage_name failed. $0", file_name));
        StorageParamPB storage_param_pb;
        RETURN_IF_ERROR(_deserialize_param(buf.ToString(), &storage_param_pb));
        RETURN_IF_ERROR(_create_remote_storage_internal(storage_param_pb));
        LOG(INFO) << "init remote_storage_param successfully. storage_name: " << file_name;
    }
    _storage_param_dir = storage_param_dir;
    _is_inited = true;
    return Status::OK();
}

Status RemoteEnvMgr::create_remote_storage(const StorageParamPB& storage_param_pb) {
    if (_check_exist(storage_param_pb)) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_create_remote_storage_internal(storage_param_pb));

    std::string storage_name = storage_param_pb.storage_name();

    string storage_param_path = _storage_param_dir + "/" + storage_name;
    RETURN_NOT_OK_STATUS_WITH_WARN(FileUtils::remove(storage_param_path),
                                   strings::Substitute("rm storage_param_pb file failed: $0", storage_param_path));
    std::string param_binary;
    RETURN_NOT_OK_STATUS_WITH_WARN(_serialize_param(storage_param_pb, &param_binary),
                                   "_serialize_param storage_param_pb failed.");
    RETURN_NOT_OK_STATUS_WITH_WARN(
            env_util::write_string_to_file(Env::Default(), Slice(param_binary), storage_param_path),
            strings::Substitute("write_string_to_file failed: $0", storage_param_path));
    faststring buf;
    RETURN_NOT_OK_STATUS_WITH_WARN(env_util::read_file_to_string(Env::Default(), storage_param_path, &buf),
                                   strings::Substitute("read storage_name failed. $0", storage_param_path));
    if (buf.ToString() != param_binary) {
        LOG(ERROR) << "storage_param written failed. storage_name: ("
                   << storage_param_pb.storage_name() << "<->" << storage_name << ")";
        return Status::InternalError("storage_param written failed");
    }
    LOG(INFO) << "create remote_storage_param successfully. storage_name: " << storage_name;
    return Status::OK();
}

Status RemoteEnvMgr::_create_remote_storage_internal(const StorageParamPB& storage_param_pb) {
    std::string storage_name = storage_param_pb.storage_name();
    WriteLock wrlock(_remote_env_lock);
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
        _remote_env_map.erase(storage_name);
        _storage_param_map.erase(storage_name);
        _remote_env_active_time.erase(storage_name);
    }
    std::shared_ptr<RemoteEnv> remote_env(new RemoteEnv());
    RETURN_NOT_OK_STATUS_WITH_WARN(remote_env->init_conf(storage_param_pb),
                                   strings::Substitute("create_remote_storage failed. storage_name: $0", storage_name));
    _storage_param_map[storage_name] = storage_param_pb;
    _remote_env_map[storage_name] = remote_env;
    _remote_env_active_time[storage_name] = time(nullptr);

    return Status::OK();
}

std::shared_ptr<RemoteEnv> RemoteEnvMgr::get_remote_env(const std::string& storage_name) {
    ReadLock rdlock(_remote_env_lock);
    if (_remote_env_map.find(storage_name) == _remote_env_map.end()) {
        return nullptr;
    }
    _remote_env_active_time[storage_name] = time(nullptr);
    return _remote_env_map[storage_name];
}

Status RemoteEnvMgr::get_storage_param(const std::string& storage_name, StorageParamPB* storage_param) {
    ReadLock rdlock(_remote_env_lock);
    if (_remote_env_map.find(storage_name) == _remote_env_map.end()) {
        return Status::InternalError("storage_name not exist: " + storage_name);
    }
    *storage_param = _storage_param_map[storage_name];
    return Status::OK();
}

Status RemoteEnvMgr::get_root_path(const std::string& storage_name, std::string* root_path) {
    ReadLock rdlock(_remote_env_lock);
    if (_remote_env_map.find(storage_name) == _remote_env_map.end()) {
        return Status::InternalError("storage_name not exist: " + storage_name);
    }
    *root_path = get_root_path_from_param(_storage_param_map[storage_name]);
    return Status::OK();
}

std::string RemoteEnvMgr::get_root_path_from_param(const StorageParamPB& storage_param) {
    switch (storage_param.storage_medium()) {
        case TStorageMedium::S3:
        default:
        {
            return storage_param.s3_storage_param().root_path();
        }
    }
}

Status RemoteEnvMgr::_check_exist(const StorageParamPB& storage_param_pb) {
    StorageParamPB old_storage_param;
    RETURN_IF_ERROR(get_storage_param(storage_param_pb.storage_name(), &old_storage_param));
    ReadLock rdlock(_remote_env_lock);
    std::string old_param_binary;
    RETURN_NOT_OK_STATUS_WITH_WARN(_serialize_param(old_storage_param, &old_param_binary),
                                   "_serialize_param old_storage_param_pb failed.");
    std::string param_binary;
    RETURN_NOT_OK_STATUS_WITH_WARN(_serialize_param(storage_param_pb, &param_binary),
                                   "_serialize_param storage_param_pb failed.");
    if (old_param_binary != param_binary) {
        LOG(ERROR) << "storage_param has been changed: " << storage_param_pb.storage_name();
        return Status::InternalError("storage_param has been changed");
    }
    return Status::OK();
}

Status RemoteEnvMgr::_serialize_param(const StorageParamPB& storage_param_pb, std::string* param_binary) {
    bool serialize_success = storage_param_pb.SerializeToString(param_binary);
    if (!serialize_success) {
        LOG(WARNING) << "failed to serialize storage_param " << storage_param_pb.storage_name();
        return Status::InternalError("failed to serialize storage_param: " + storage_param_pb.storage_name());
    }
    return Status::OK();
}

Status RemoteEnvMgr::_deserialize_param(const std::string& param_binary, StorageParamPB* storage_param_pb) {
    bool parsed = storage_param_pb->ParseFromString(param_binary);
    if (!parsed) {
        LOG(WARNING) << "parse storage_param failed";
        return Status::InternalError("parse storage_param failed");
    }
    return Status::OK();
}

} // namespace doris