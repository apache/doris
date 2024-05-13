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

#include <bvar/bvar.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>

#include "common/status.h"
#include "io/fs/hdfs.h"
#include "io/fs/path.h"

namespace cloud {
class HdfsVaultInfo;
}

namespace doris {
class HDFSCommonBuilder;
class THdfsParams;

namespace io {

namespace hdfs_bvar {
extern bvar::LatencyRecorder hdfs_read_latency;
extern bvar::LatencyRecorder hdfs_write_latency;
extern bvar::LatencyRecorder hdfs_create_dir_latency;
extern bvar::LatencyRecorder hdfs_open_latency;
extern bvar::LatencyRecorder hdfs_close_latency;
extern bvar::LatencyRecorder hdfs_flush_latency;
extern bvar::LatencyRecorder hdfs_hflush_latency;
extern bvar::LatencyRecorder hdfs_hsync_latency;
}; // namespace hdfs_bvar

class HdfsHandler {
public:
    HdfsHandler(hdfsFS fs, bool cached)
            : hdfs_fs(fs),
              from_cache(cached),
              _create_time(std::chrono::duration_cast<std::chrono::milliseconds>(
                                   std::chrono::system_clock::now().time_since_epoch())
                                   .count()),
              _last_access_time(0),
              _invalid(false) {}

    ~HdfsHandler() {
        if (hdfs_fs != nullptr) {
            // DO NOT call hdfsDisconnect(), or we will meet "Filesystem closed"
            // even if we create a new one
            // hdfsDisconnect(hdfs_fs);
        }
        hdfs_fs = nullptr;
    }

    int64_t last_access_time() { return _last_access_time; }

    void update_last_access_time() {
        if (from_cache) {
            _last_access_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                                        std::chrono::system_clock::now().time_since_epoch())
                                        .count();
        }
    }

    bool invalid() { return _invalid; }

    void set_invalid() { _invalid = true; }

    hdfsFS hdfs_fs;
    // When cache is full, and all handlers are in use, HdfsFileSystemCache will return an uncached handler.
    // Client should delete the handler in such case.
    const bool from_cache;

private:
    // For kerberos authentication, we need to save create time so that
    // we can know if the kerberos ticket is expired.
    std::atomic<uint64_t> _create_time;
    // HdfsFileSystemCache try to remove the oldest handler when the cache is full
    std::atomic<uint64_t> _last_access_time;
    // Client will set invalid if error thrown, and HdfsFileSystemCache will not reuse this handler
    std::atomic<bool> _invalid;
};

// Cache for HdfsHandler
class HdfsHandlerCache {
public:
    static HdfsHandlerCache* instance() {
        static HdfsHandlerCache s_instance;
        return &s_instance;
    }

    HdfsHandlerCache(const HdfsHandlerCache&) = delete;
    const HdfsHandlerCache& operator=(const HdfsHandlerCache&) = delete;

    // This function is thread-safe
    Status get_connection(const THdfsParams& hdfs_params, const std::string& fs_name,
                          std::shared_ptr<HdfsHandler>* fs_handle);

private:
    static constexpr int MAX_CACHE_HANDLE = 64;

    std::mutex _lock;
    std::unordered_map<uint64_t, std::shared_ptr<HdfsHandler>> _cache;

    HdfsHandlerCache() = default;

    void _clean_invalid();
    void _clean_oldest();
};

// if the format of path is hdfs://ip:port/path, replace it to /path.
// path like hdfs://ip:port/path can't be used by libhdfs3.
Path convert_path(const Path& path, const std::string& namenode);

std::string get_fs_name(const std::string& path);

// return true if path_or_fs contains "hdfs://"
bool is_hdfs(const std::string& path_or_fs);

THdfsParams to_hdfs_params(const cloud::HdfsVaultInfo& vault);

} // namespace io
} // namespace doris
