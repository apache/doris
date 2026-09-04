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

#include "format_v2/lance/lance_session_manager.h"

#include <lance/lance.h>

#include <string>
#include <utility>

#include "common/config.h"
#include "common/logging.h"
#include "format_v2/lance/lance_reader_helper.h"

namespace doris::format::lance {
namespace {

LanceSessionManager::Config load_lance_session_config() {
    return {
            .lance_index_cache_size_bytes = config::lance_index_cache_size_bytes,
            .lance_metadata_cache_size_bytes = config::lance_metadata_cache_size_bytes,
            .enable_lance_data_cache = config::enable_lance_data_cache,
            .lance_data_cache_path = config::lance_data_cache_path,
            .lance_data_cache_disk_capacity_bytes = config::lance_data_cache_disk_capacity_bytes,
            .lance_data_cache_read_block_size_bytes =
                    config::lance_data_cache_read_block_size_bytes,
    };
}

} // namespace

LanceSessionManager& LanceSessionManager::instance() {
    // Function-local static initialization is thread safe. Cache configuration is process scoped,
    // so changing it requires a BE restart.
    static LanceSessionManager manager(load_lance_session_config());
    return manager;
}

LanceSessionManager::LanceSessionManager(Config config) : _config(std::move(config)) {
    LOG(INFO) << "Creating BE-wide Lance session manager: lance_index_cache_size_bytes="
              << _config.lance_index_cache_size_bytes
              << ", lance_metadata_cache_size_bytes="
              << _config.lance_metadata_cache_size_bytes
              << ", enable_lance_data_cache=" << _config.enable_lance_data_cache
              << ", lance_data_cache_path=" << _config.lance_data_cache_path
              << ", lance_data_cache_disk_capacity_bytes="
              << _config.lance_data_cache_disk_capacity_bytes
              << ", lance_data_cache_read_block_size_bytes="
              << _config.lance_data_cache_read_block_size_bytes
              << ", foyer_memory_capacity_bytes="
              << _config.lance_data_cache_read_block_size_bytes;
}

LanceSessionManager::~LanceSessionManager() {
    lance_session_close(_session);
}

Status LanceSessionManager::_initialize() {
    if (_config.enable_lance_data_cache) {
        const LanceDataCacheOptions data_cache_options {
                .directory = _config.lance_data_cache_path.c_str(),
                // Foyer's HybridCache requires a memory tier. Keep it at the minimum useful
                // capacity of exactly one range-cache block; entries use WriteOnInsertion and
                // are persisted to the disk tier immediately.
                .memory_capacity_bytes =
                        static_cast<uint64_t>(_config.lance_data_cache_read_block_size_bytes),
                .disk_capacity_bytes =
                        static_cast<uint64_t>(_config.lance_data_cache_disk_capacity_bytes),
                .read_block_size_bytes =
                        static_cast<uint64_t>(_config.lance_data_cache_read_block_size_bytes),
        };
        _session = lance_session_new_with_data_cache(
                static_cast<uint64_t>(_config.lance_index_cache_size_bytes),
                static_cast<uint64_t>(_config.lance_metadata_cache_size_bytes),
                &data_cache_options);
    } else {
        _session = lance_session_new(
                static_cast<uint64_t>(_config.lance_index_cache_size_bytes),
                static_cast<uint64_t>(_config.lance_metadata_cache_size_bytes));
    }
    if (_session == nullptr) {
        return lance_error("create shared Lance session");
    }
    return Status::OK();
}

Status LanceSessionManager::open_dataset(const char* uri, const char* const* storage_options,
                                         uint64_t version, LanceDataset** dataset) {
    if (uri == nullptr || dataset == nullptr) {
        return Status::InvalidArgument("Lance dataset URI and output must not be null");
    }
    *dataset = nullptr;

    std::call_once(_initialize_once, [this] { _initialize_status = _initialize(); });
    RETURN_IF_ERROR(_initialize_status);

    *dataset = lance_dataset_open_with_session(uri, storage_options, version, _session);
    if (*dataset == nullptr) {
        return lance_error("open Lance dataset with shared session");
    }
    return Status::OK();
}

} // namespace doris::format::lance
