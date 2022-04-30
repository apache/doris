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

#include "util/storage_backend_mgr.h"

#include "common/config.h"
#include "common/status.h"
#include "env/env.h"
#include "env/env_util.h"
#include "gutil/strings/substitute.h"
#include "util/file_utils.h"
#include "util/s3_storage_backend.h"
#include "util/s3_util.h"
#include "util/storage_backend.h"

namespace doris {

Status StorageBackendMgr::init(const std::string& storage_param_dir) {
    if (_is_inited) {
        return Status::OK();
    }
    Status exist_status = Env::Default()->path_exists(storage_param_dir);
    if (!exist_status.ok() &&
        (!exist_status.is_not_found() || !Env::Default()->create_dirs(storage_param_dir).ok())) {
        RETURN_NOT_OK_STATUS_WITH_WARN(
                Status::IOError(strings::Substitute(
                        "failed to create remote storage_param root path $0", storage_param_dir)),
                "create_dirs failed");
    }

    std::vector<std::string> file_names;
    RETURN_IF_ERROR(FileUtils::list_files(Env::Default(), storage_param_dir, &file_names));
    for (auto& file_name : file_names) {
        std::string buf;
        RETURN_NOT_OK_STATUS_WITH_WARN(
                env_util::read_file_to_string(Env::Default(), storage_param_dir + "/" + file_name,
                                              &buf),
                strings::Substitute("load storage_name failed. $0", file_name));
        StorageParamPB storage_param_pb;
        RETURN_IF_ERROR(_deserialize_param(buf, &storage_param_pb));
        RETURN_IF_ERROR(_create_remote_storage_internal(storage_param_pb));
        LOG(INFO) << "init remote_storage_param successfully. storage_name: " << file_name;
    }
    _storage_param_dir = storage_param_dir;
    _is_inited = true;
    return Status::OK();
}

Status StorageBackendMgr::create_remote_storage(const StorageParamPB& storage_param_pb) {
    if (_check_exist(storage_param_pb)) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_create_remote_storage_internal(storage_param_pb));

    std::string storage_name = storage_param_pb.storage_name();

    string storage_param_path = _storage_param_dir + "/" + storage_name;
    RETURN_NOT_OK_STATUS_WITH_WARN(
            FileUtils::remove(storage_param_path),
            strings::Substitute("rm storage_param_pb file failed: $0", storage_param_path));
    std::string param_binary;
    RETURN_NOT_OK_STATUS_WITH_WARN(_serialize_param(storage_param_pb, &param_binary),
                                   "_serialize_param storage_param_pb failed.");
    RETURN_NOT_OK_STATUS_WITH_WARN(
            env_util::write_string_to_file(Env::Default(), Slice(param_binary), storage_param_path),
            strings::Substitute("write_string_to_file failed: $0", storage_param_path));
    std::string buf;
    RETURN_NOT_OK_STATUS_WITH_WARN(
            env_util::read_file_to_string(Env::Default(), storage_param_path, &buf),
            strings::Substitute("read storage_name failed. $0", storage_param_path));
    if (buf != param_binary) {
        LOG(ERROR) << "storage_param written failed. storage_name: ("
                   << storage_param_pb.storage_name() << "<->" << storage_name << ")";
        return Status::InternalError("storage_param written failed");
    }
    LOG(INFO) << "create remote_storage_param successfully. storage_name: " << storage_name;
    return Status::OK();
}

Status StorageBackendMgr::_create_remote_storage_internal(const StorageParamPB& storage_param_pb) {
    std::string storage_name = storage_param_pb.storage_name();
    std::unique_lock wrlock(_storage_backend_lock);
    if (_storage_backend_map.size() >= doris::config::max_remote_storage_count) {
        std::map<std::string, time_t>::iterator itr = _storage_backend_active_time.begin();
        std::string timeout_key = itr->first;
        time_t min_active_time = itr->second;
        ++itr;
        for (; itr != _storage_backend_active_time.end(); ++itr) {
            if (itr->second < min_active_time) {
                timeout_key = itr->first;
                min_active_time = itr->second;
            }
        }
        _storage_backend_map.erase(storage_name);
        _storage_param_map.erase(storage_name);
        _storage_backend_active_time.erase(storage_name);
    }
    std::map<std::string, std::string> storage_prop;
    switch (storage_param_pb.storage_medium()) {
    case StorageMediumPB::S3:
    default:
        S3StorageParamPB s3_storage_param = storage_param_pb.s3_storage_param();
        if (s3_storage_param.s3_ak().empty() || s3_storage_param.s3_sk().empty() ||
            s3_storage_param.s3_endpoint().empty() || s3_storage_param.s3_region().empty()) {
            return Status::InternalError("s3_storage_param param is invalid");
        }
        storage_prop[S3_AK] = s3_storage_param.s3_ak();
        storage_prop[S3_SK] = s3_storage_param.s3_sk();
        storage_prop[S3_ENDPOINT] = s3_storage_param.s3_endpoint();
        storage_prop[S3_REGION] = s3_storage_param.s3_region();
        storage_prop[S3_MAX_CONN_SIZE] = s3_storage_param.s3_max_conn();
        storage_prop[S3_REQUEST_TIMEOUT_MS] = s3_storage_param.s3_request_timeout_ms();
        storage_prop[S3_CONN_TIMEOUT_MS] = s3_storage_param.s3_conn_timeout_ms();

        if (!ClientFactory::is_s3_conf_valid(storage_prop)) {
            return Status::InternalError("s3_storage_param is invalid");
        }
        _storage_backend_map[storage_name] = std::make_shared<S3StorageBackend>(storage_prop);
    }
    _storage_param_map[storage_name] = storage_param_pb;
    _storage_backend_active_time[storage_name] = time(nullptr);

    return Status::OK();
}

std::shared_ptr<StorageBackend> StorageBackendMgr::get_storage_backend(
        const std::string& storage_name) {
    std::shared_lock rdlock(_storage_backend_lock);
    if (_storage_backend_map.find(storage_name) == _storage_backend_map.end()) {
        return nullptr;
    }
    _storage_backend_active_time[storage_name] = time(nullptr);
    return _storage_backend_map[storage_name];
}

Status StorageBackendMgr::get_storage_param(const std::string& storage_name,
                                            StorageParamPB* storage_param) {
    std::shared_lock rdlock(_storage_backend_lock);
    if (_storage_backend_map.find(storage_name) == _storage_backend_map.end()) {
        return Status::InternalError("storage_name not exist: " + storage_name);
    }
    *storage_param = _storage_param_map[storage_name];
    return Status::OK();
}

Status StorageBackendMgr::get_root_path(const std::string& storage_name, std::string* root_path) {
    std::shared_lock rdlock(_storage_backend_lock);
    if (_storage_backend_map.find(storage_name) == _storage_backend_map.end()) {
        return Status::InternalError("storage_name not exist: " + storage_name);
    }
    *root_path = get_root_path_from_param(_storage_param_map[storage_name]);
    return Status::OK();
}

std::string StorageBackendMgr::get_root_path_from_param(const StorageParamPB& storage_param) {
    switch (storage_param.storage_medium()) {
    case StorageMediumPB::S3:
    default: {
        return storage_param.s3_storage_param().root_path();
    }
    }
}

Status StorageBackendMgr::_check_exist(const StorageParamPB& storage_param_pb) {
    StorageParamPB old_storage_param;
    RETURN_IF_ERROR(get_storage_param(storage_param_pb.storage_name(), &old_storage_param));
    std::shared_lock rdlock(_storage_backend_lock);
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

Status StorageBackendMgr::_serialize_param(const StorageParamPB& storage_param_pb,
                                           std::string* param_binary) {
    bool serialize_success = storage_param_pb.SerializeToString(param_binary);
    if (!serialize_success) {
        LOG(WARNING) << "failed to serialize storage_param " << storage_param_pb.storage_name();
        return Status::InternalError("failed to serialize storage_param: " +
                                     storage_param_pb.storage_name());
    }
    return Status::OK();
}

Status StorageBackendMgr::_deserialize_param(const std::string& param_binary,
                                             StorageParamPB* storage_param_pb) {
    bool parsed = storage_param_pb->ParseFromString(param_binary);
    if (!parsed) {
        LOG(WARNING) << "parse storage_param failed";
        return Status::InternalError("parse storage_param failed");
    }
    return Status::OK();
}

} // namespace doris