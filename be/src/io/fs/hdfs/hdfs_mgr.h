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

#include <gen_cpp/PlanNodes_types.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include "common/status.h"
#include "io/fs/hdfs.h"
#include "io/hdfs_util.h"

namespace doris::io {

// A manager class to handle multiple hdfsFS instances
class HdfsMgr {
public:
    HdfsMgr();

    // Get or create a hdfsFS instance based on the given parameters
    Status get_or_create_fs(const THdfsParams& hdfs_params, const std::string& fs_name,
                            std::shared_ptr<HdfsHandler>* fs_handler);

    virtual ~HdfsMgr();

protected:
    // For testing purpose
    friend class HdfsMgrTest;
    size_t get_fs_handlers_size() const { return _fs_handlers.size(); }
    bool has_fs_handler(uint64_t hash_code) const {
        return _fs_handlers.find(hash_code) != _fs_handlers.end();
    }
    void set_instance_timeout_seconds(int64_t timeout_seconds) {
        _instance_timeout_seconds = timeout_seconds;
    }
    void set_cleanup_interval_seconds(int64_t interval_seconds) {
        _cleanup_interval_seconds = interval_seconds;
    }
    uint64_t _hdfs_hash_code(const THdfsParams& hdfs_params, const std::string& fs_name);

    virtual Status _create_hdfs_fs_impl(const THdfsParams& hdfs_params, const std::string& fs_name,
                                        std::shared_ptr<HdfsHandler>* fs_handler);

private:
    HdfsMgr(const HdfsMgr&) = delete;
    HdfsMgr& operator=(const HdfsMgr&) = delete;

    // Start the cleanup thread
    void _start_cleanup_thread();
    // Stop the cleanup thread
    void _stop_cleanup_thread();
    // Cleanup thread function
    void _cleanup_loop();
    // Remove kerberos ticket cache if instance is using kerberos auth
    void _cleanup_kerberos_ticket(const HdfsHandler& handler);

    Status _create_hdfs_fs(const THdfsParams& hdfs_params, const std::string& fs_name,
                           std::shared_ptr<HdfsHandler>* fs_handler);

private:
    std::mutex _mutex;
    std::unordered_map<uint64_t, std::shared_ptr<HdfsHandler>> _fs_handlers;

    std::atomic<bool> _should_stop_cleanup_thread;
    std::unique_ptr<std::thread> _cleanup_thread;
    int64_t _cleanup_interval_seconds = 3600;  // Run cleanup every hour
    int64_t _instance_timeout_seconds = 86400; // 24 hours timeout
};

} // namespace doris::io
