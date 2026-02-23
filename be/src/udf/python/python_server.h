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

#include <atomic>
#include <condition_variable>
#include <memory>
#include <thread>

#include "common/status.h"
#include "runtime/memory/mem_tracker.h"
#include "udf/python/python_udf_meta.h"
#include "udf/python/python_udf_runtime.h"

namespace doris {
class PythonServerManager {
public:
    PythonServerManager() = default;

    ~PythonServerManager() { shutdown(); }

    static PythonServerManager& instance() {
        static PythonServerManager instance;
        return instance;
    }

    template <typename T>
    Status get_client(const PythonUDFMeta& func_meta, const PythonVersion& version,
                      std::shared_ptr<T>* client,
                      const std::shared_ptr<arrow::Schema>& data_schema = nullptr);

    Status fork(const PythonVersion& version, ProcessPtr* process);

    Status get_process(const PythonVersion& version, ProcessPtr* process);

    Status ensure_pool_initialized(const PythonVersion& version);

    void shutdown();

#ifdef BE_TEST
    // For unit testing only.
    void check_and_recreate_processes_for_test() { _check_and_recreate_processes(); }

    std::unordered_map<PythonVersion, std::vector<ProcessPtr>>& process_pools_for_test() {
        return _process_pools;
    }
#endif

private:
    /**
     * Start health check background thread (called once by ensure_pool_initialized)
     * Thread periodically checks process health and refreshes memory stats
     */
    void _start_health_check_thread();

    /**
     * Check process health and recreate dead processes
     */
    void _check_and_recreate_processes();

    /**
     * Read resident set size (RSS) for a single process from /proc/{pid}/statm
     */
    Status _read_process_memory(pid_t pid, size_t* rss_bytes);

    /**
     * Refresh memory statistics for all Python processes
     */
    void _refresh_memory_stats();

    std::unordered_map<PythonVersion, std::vector<ProcessPtr>> _process_pools;
    // Protects _process_pools access
    std::mutex _pools_mutex;
    // Track which versions have been initialized
    std::unordered_set<PythonVersion> _initialized_versions;
    // Health check background thread
    std::unique_ptr<std::thread> _health_check_thread;
    std::atomic<bool> _shutdown_flag {false};
    std::condition_variable _health_check_cv;
    std::mutex _health_check_mutex;
    MemTracker _mem_tracker {"PythonUDFProcesses"};
};

} // namespace doris