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
        : SpillDataDir(fmt::format("s3:{}", vault_id.empty() ? "default" : vault_id),
                       /*spill_root=*/"",
                       fmt::format("s3:{}", vault_id.empty() ? "default" : vault_id),
                       /*capacity_bytes=*/0, TStorageMedium::S3),
          _vault_id(std::move(vault_id)) {}

Status RemoteSpillDataDir::init() {
    RETURN_IF_ERROR(update_capacity());
    LOG(INFO) << fmt::format("remote spill store registered, vault_id={}, limit={}",
                             _vault_id.empty() ? "<default>" : _vault_id,
                             PrettyPrinter::print_bytes(_spill_data_limit_bytes));
    return Status::OK();
}

Status RemoteSpillDataDir::ensure_ready() {
    if (ready()) {
        return Status::OK();
    }
    std::lock_guard<std::mutex> lock(_init_mutex);
    if (ready()) {
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
    // The vault is resolved from what the vault refresh thread already brought in.
    auto& engine = ExecEnv::GetInstance()->storage_engine().to_cloud();
    std::string vault_id = _vault_id.empty() ? engine.default_vault_id() : _vault_id;
    io::RemoteFileSystemSPtr fs =
            vault_id.empty() ? engine.latest_fs() : doris::get_filesystem(vault_id);
    if (fs == nullptr) {
        return Status::InternalError(
                "spill to s3 is not ready: storage vault '{}' not found (empty means the default "
                "vault of the instance; set spill_s3_storage_vault to the vault ID in be.conf if "
                "the instance has no default vault)",
                vault_id);
    }
    if (fs->type() != io::FileSystemType::S3) {
        return Status::NotSupported("spill to s3 only supports S3 storage vaults, vault '{}' is {}",
                                    vault_id, fs->type());
    }
    init_remote_fs(fs, std::move(endpoint));
    return Status::OK();
}

void RemoteSpillDataDir::init_remote_fs(io::FileSystemSPtr fs, std::string endpoint) {
    DCHECK(!endpoint.empty());
    _fs = std::move(fs);
    _endpoint = std::move(endpoint);
    _spill_root = fmt::format("{}/{}", SPILL_DIR_PREFIX, _endpoint);
    _ready.store(true, std::memory_order_release);
    LOG(INFO) << fmt::format(
            "remote spill store is ready, vault_id={}, fs_id={}, root={}, limit={}",
            _vault_id.empty() ? "<default>" : _vault_id, _fs->id(), _spill_root,
            PrettyPrinter::print_bytes(config::spill_s3_storage_limit_bytes));
}

io::FileSystemSPtr RemoteSpillDataDir::fs() const {
    return ready() ? _fs : nullptr;
}

Status RemoteSpillDataDir::update_capacity() {
    std::lock_guard<std::mutex> l(_mutex);
    _disk_capacity_bytes = 0;
    _available_bytes = 0;
    _spill_data_limit_bytes = config::spill_s3_storage_limit_bytes;
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
