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

#include "exec/spill/remote_spill_data_dir.h"

#include <glog/logging.h>

#include <utility>

#include "cloud/cloud_storage_engine.h"
#include "cloud/config.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/metrics/metrics.h"
#include "io/fs/remote_file_system.h"
#include "runtime/exec_env.h"
#include "service/backend_options.h"
#include "storage/olap_define.h"
#include "storage/storage_policy.h"
#include "util/pretty_printer.h"

namespace doris {

RemoteSpillDataDir::RemoteSpillDataDir(std::string vault_id)
        : SpillDataDir(fmt::format("s3:{}", vault_id.empty() ? "auto" : vault_id),
                       /*spill_root=*/"",
                       fmt::format("s3:{}", vault_id.empty() ? "auto" : vault_id),
                       /*capacity_bytes=*/0, TStorageMedium::S3),
          _vault_id(std::move(vault_id)) {}

Status RemoteSpillDataDir::init() {
    RETURN_IF_ERROR(update_capacity());
    LOG(INFO) << fmt::format("remote spill store registered, vault_id={}, limit={}",
                             _vault_id.empty() ? "<auto>" : _vault_id,
                             PrettyPrinter::print_bytes(_spill_data_limit_bytes));
    return Status::OK();
}

io::RemoteFileSystemSPtr RemoteSpillDataDir::select_vault_fs(
        const std::string& vault_id, const io::RemoteFileSystemSPtr& latest_fs) {
    if (!vault_id.empty()) {
        return get_filesystem(vault_id);
    }
    if (latest_fs != nullptr) {
        // Only an instance without storage vaults has a latest file system.
        return latest_fs;
    }
    // Storage vault mode: the S3 vault with the largest ID, i.e. the newest one. Changing the
    // default vault does not move spill; adding an S3 vault moves new spill files to it.
    io::RemoteFileSystemSPtr selected;
    std::string selected_id;
    for (const auto& id_and_version : get_storage_resource_ids()) {
        const std::string& id = id_and_version.first;
        auto resource = get_storage_resource(id);
        // The resource map may change between listing and lookup.
        if (!resource.has_value() || resource->first.fs->type() != io::FileSystemType::S3) {
            continue;
        }
        // Vault IDs are decimal numbers: a longer ID is a larger one.
        if (selected == nullptr || id.size() > selected_id.size() ||
            (id.size() == selected_id.size() && id > selected_id)) {
            selected = resource->first.fs;
            selected_id = id;
        }
    }
    return selected;
}

Status RemoteSpillDataDir::ensure_ready(io::FileSystemSPtr* file_fs) {
    std::lock_guard<std::mutex> lock(_init_mutex);
    if (_test_binding || (!_vault_id.empty() && ready())) {
        *file_fs = _fs;
        return Status::OK();
    }
    if (!config::is_cloud_mode()) {
        return Status::InternalError("spill to s3 is only supported in cloud mode");
    }
    const std::string& host = BackendOptions::get_localhost();
    if (host.empty()) {
        return Status::InternalError("spill to s3 is not ready: the address of this BE is unknown");
    }
    std::string endpoint = fmt::format("{}_{}", host, config::heartbeat_service_port);
    // The vaults are the ones the vault refresh thread of the storage engine already brought in.
    io::RemoteFileSystemSPtr fs = select_vault_fs(
            _vault_id, ExecEnv::GetInstance()->storage_engine().to_cloud().latest_fs());
    if (fs == nullptr) {
        return Status::InternalError(
                "spill to s3 is not ready: {}",
                _vault_id.empty() ? "no S3 storage vault is known yet"
                                  : fmt::format("storage vault '{}' not found", _vault_id));
    }
    if (fs->type() != io::FileSystemType::S3) {
        return Status::NotSupported("spill to s3 only supports S3 storage vaults, vault '{}' is {}",
                                    _vault_id, fs->type());
    }
    if (!ready() || _fs != fs) {
        _bind_fs_unlocked(fs, std::move(endpoint));
    }
    *file_fs = _fs;
    return Status::OK();
}

void RemoteSpillDataDir::init_remote_fs(io::FileSystemSPtr fs, std::string endpoint) {
    std::lock_guard<std::mutex> lock(_init_mutex);
    _test_binding = true;
    _bind_fs_unlocked(std::move(fs), std::move(endpoint));
}

void RemoteSpillDataDir::_bind_fs_unlocked(io::FileSystemSPtr fs, std::string endpoint) {
    DCHECK(!endpoint.empty());
    _fs = std::move(fs);
    if (!ready()) {
        _endpoint = std::move(endpoint);
        _spill_root = fmt::format("{}/{}", SPILL_DIR_PREFIX, _endpoint);
    }
    _ready.store(true, std::memory_order_release);
    LOG(INFO) << fmt::format(
            "remote spill store is ready, vault_id={}, fs_id={}, root={}, limit={}",
            _vault_id.empty() ? "<auto>" : _vault_id, _fs->id(), _spill_root,
            PrettyPrinter::print_bytes(config::spill_storage_limit_bytes));
}

io::FileSystemSPtr RemoteSpillDataDir::fs() const {
    std::lock_guard<std::mutex> lock(_init_mutex);
    return _fs;
}

void RemoteSpillDataDir::record_persisted_bytes(int64_t bytes) {
    std::lock_guard<std::mutex> lock(_mutex);
    _persisted_bytes += bytes;
}

void RemoteSpillDataDir::release_persisted_bytes(int64_t bytes) {
    std::lock_guard<std::mutex> lock(_mutex);
    DCHECK_GE(_persisted_bytes, bytes);
    _persisted_bytes -= bytes;
}

int64_t RemoteSpillDataDir::get_persisted_bytes() {
    std::lock_guard<std::mutex> lock(_mutex);
    return _persisted_bytes;
}

Status RemoteSpillDataDir::update_capacity() {
    std::lock_guard<std::mutex> l(_mutex);
    _disk_capacity_bytes = 0;
    _available_bytes = 0;
    _spill_data_limit_bytes = config::spill_storage_limit_bytes;
    spill_disk_capacity->set_value(0);
    spill_disk_avail_capacity->set_value(0);
    spill_disk_limit->set_value(_spill_data_limit_bytes);
    spill_disk_has_spill_data->set_value(_spill_data_bytes > 0 ? 1 : 0);
    spill_disk_has_spill_gc_data->set_value(0);
    return Status::OK();
}

bool RemoteSpillDataDir::_reach_limit_unlocked(int64_t incoming_data_size) {
    // 0 means unlimited.
    if (_spill_data_limit_bytes > 0 &&
        _spill_data_bytes + incoming_data_size > _spill_data_limit_bytes) {
        LOG_EVERY_T(WARNING, 1) << fmt::format(
                "remote spill data reach limit, store: {}, limit: {}, used: {}, incoming "
                "bytes: {}",
                _path, PrettyPrinter::print_bytes(_spill_data_limit_bytes),
                PrettyPrinter::print_bytes(_spill_data_bytes),
                PrettyPrinter::print_bytes(incoming_data_size));
        return true;
    }
    return false;
}

} // namespace doris
