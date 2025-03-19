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

// #include "common/kerberos/kerberos_ticket_cache.h"
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

class HdfsHandler {
public:
    hdfsFS hdfs_fs;
    bool is_kerberos_auth;
    std::string principal;
    std::string keytab_path;
    std::string fs_name;
    uint64_t create_time;
    std::atomic<uint64_t> last_access_time;
    // std::shared_ptr<kerberos::KerberosTicketCache> ticket_cache;

    HdfsHandler(hdfsFS fs, bool is_kerberos, const std::string& principal_,
                const std::string& keytab_path_, const std::string& fs_name_)
            // std::shared_ptr<kerberos::KerberosTicketCache> ticket_cache_)
            : hdfs_fs(fs),
              is_kerberos_auth(is_kerberos),
              principal(principal_),
              keytab_path(keytab_path_),
              fs_name(fs_name_),
              create_time(std::time(nullptr)),
              last_access_time(std::time(nullptr)) {}
    // ticket_cache(ticket_cache_) {}

    ~HdfsHandler() {
        // The ticket_cache will be automatically released when the last reference is gone
        // No need to explicitly cleanup kerberos ticket
    }

    void update_access_time() { last_access_time = std::time(nullptr); }
};

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

// if the format of path is hdfs://ip:port/path, replace it to /path.
// path like hdfs://ip:port/path can't be used by libhdfs3.
Path convert_path(const Path& path, const std::string& namenode);

std::string get_fs_name(const std::string& path);

// return true if path_or_fs contains "hdfs://"
bool is_hdfs(const std::string& path_or_fs);

THdfsParams to_hdfs_params(const cloud::HdfsVaultInfo& vault);

} // namespace io
} // namespace doris
