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

#include <filesystem>
#include <functional>
#include <optional>
#include <string>

#include "common/status.h"
#include "io/cache/file_cache_common.h"

namespace doris::io {

struct DiskScanFileIdentity {
    uint64_t dev = 0;
    uint64_t ino = 0;
    uint64_t size = 0;
    int64_t mtime_sec = 0;
};

struct DiskScanKeyDirEntry {
    std::string prefix;
    UInt128Wrapper hash;
    uint64_t expiration_time = 0;
    std::filesystem::path key_dir;
};

struct DiskScanBlockFileEntry {
    UInt128Wrapper hash;
    size_t offset = 0;
    uint64_t expiration_time = 0;
    FileCacheType cache_type = FileCacheType::NORMAL;
    bool is_tmp = false;
    DiskScanFileIdentity identity;
    std::filesystem::path file_path;
    std::filesystem::path key_dir;
};

using DiskScanKeyDirCallback = std::function<Status(const DiskScanKeyDirEntry&)>;
using DiskScanBlockFileCallback = std::function<Status(const DiskScanBlockFileEntry&)>;

enum class DiskScanRepairActionType {
    DELETE_FILE,
    DELETE_DIR,
};

enum class DiskScanRepairReason {
    DISK_ONLY_FILE,
    OLD_TMP_FILE,
    TTL_DUPLICATE_DIR,
};

struct DiskScanRepairAction {
    DiskScanRepairActionType type = DiskScanRepairActionType::DELETE_FILE;
    UInt128Wrapper hash;
    std::optional<size_t> offset;
    uint64_t expiration_time = 0;
    std::filesystem::path path;
    std::filesystem::path key_dir;
    std::optional<DiskScanFileIdentity> observed_identity;
    DiskScanRepairReason reason = DiskScanRepairReason::DISK_ONLY_FILE;
};

struct DiskScanConfig {
    int64_t grace_seconds = 0;
};

struct DiskScanRoundResult {
    size_t scanned_prefix_dirs = 0;
    size_t scanned_key_dirs = 0;
    size_t scanned_files = 0;
    size_t candidates = 0;
    size_t repaired_files = 0;
    size_t repaired_dirs = 0;
    size_t skipped_candidates = 0;
    size_t deleted_bytes = 0;
};

} // namespace doris::io
