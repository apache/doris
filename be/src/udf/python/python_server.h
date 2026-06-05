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
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

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

    static Status fork(const PythonVersion& version, ProcessPtr* process);

    // Clear Python module cache for a specific UDF location across all processes
    Status clear_module_cache(const std::string& location);

    // Clear Python UDAF runtime state after DROP FUNCTION
    void clear_udaf_state_cache(int64_t function_id);

    void shutdown();

#ifdef BE_TEST
    // For unit testing only.
    void check_and_recreate_processes_for_test() { _check_and_recreate_processes(); }

    void set_process_pool_for_test(const PythonVersion& version, std::vector<ProcessPtr> processes,
                                   bool initialized = true);

    std::vector<ProcessPtr> process_pool_snapshot_for_test(const PythonVersion& version);

    bool process_pool_is_initializing_for_test(const PythonVersion& version);

    bool process_pool_is_initialized_for_test(const PythonVersion& version);

    Status broadcast_action_to_processes_for_test(const std::string& action_type,
                                                  const std::string& body,
                                                  const std::string& log_name) {
        return _broadcast_action_to_processes(action_type, body, log_name);
    }
#endif

private:
    enum class PoolState {
        UNINITIALIZED,
        INITIALIZING,
        // `Initialized` means:
        // 1. All process slots have attempted initialization.
        // 2. At least one live process is available for requests.
        // 3. The health-check thread has been started.
        INITIALIZED,
        // Prevent the pool from being incorrectly reused after `shutdown()`
        STOPPED,
    };

    struct VersionedProcessPool {
        std::mutex mutex;
        // Coordinates initialization and repair workers with foreground requests.
        std::condition_variable cv;
        std::vector<ProcessPtr> processes;
        PoolState state = PoolState::UNINITIALIZED;
        // True when at least one process in the pool can serve requests.
        bool has_available_process = false;
        // True while a background repair is recreating dead or missing processes.
        bool repairing = false;
    };

    /** 
     * Lazily initialize and return the process pool for specific Python version. 
     */
    Result<std::shared_ptr<VersionedProcessPool>> _ensure_pool_initialized(
            const PythonVersion& version);

    /**
     * Pick an available process from specific pool, recreating one on demand if needed.
     */
    Status _get_process(const PythonVersion& version,
                        const std::shared_ptr<VersionedProcessPool>& versioned_pool,
                        ProcessPtr* process);

    /**
     * Start health check background thread (called once by ensure_pool_initialized)
     * Thread periodically checks process health and refreshes memory stats
     */
    void _start_health_check_thread();

    /**
     * Check process health and recreate dead processes
     */
    void _check_and_recreate_processes();

#ifdef BE_TEST
    static constexpr std::chrono::milliseconds PROCESS_START_TIMEOUT {500};
    static constexpr std::chrono::milliseconds PROCESS_TERMINATE_TIMEOUT {100};
    static constexpr std::chrono::milliseconds PROCESS_POOL_INIT_TIMEOUT {1000};
    static constexpr std::chrono::milliseconds PROCESS_REPAIR_WAIT_TIMEOUT {200};
#else
    static constexpr std::chrono::milliseconds PROCESS_START_TIMEOUT {5000};
    static constexpr std::chrono::milliseconds PROCESS_TERMINATE_TIMEOUT {1000};
    // FE's default send-fragments RPC timeout is 30s. Keep BE's Python pool wait below it so the
    // caller sees SERVICE_UNAVAILABLE with Python context instead of a generic RPC deadline error.
    static constexpr std::chrono::milliseconds PROCESS_POOL_INIT_TIMEOUT {20000};
    static constexpr std::chrono::milliseconds PROCESS_REPAIR_WAIT_TIMEOUT {1000};
#endif
    static int _repair_process_pool(const PythonVersion& version,
                                    const std::shared_ptr<VersionedProcessPool>& versioned_pool);

    /**
     * Read resident set size (RSS) for a single process from /proc/{pid}/statm
     */
    Status _read_process_memory(pid_t pid, size_t* rss_bytes);

    /**
     * Refresh memory statistics for all Python processes
     */
    void _refresh_memory_stats();

    Result<std::shared_ptr<VersionedProcessPool>> _get_or_create_process_pool(
            const PythonVersion& version);
    std::vector<std::pair<PythonVersion, std::shared_ptr<VersionedProcessPool>>>
    _snapshot_process_pools();
    Status _broadcast_action_to_processes(const std::string& action_type, const std::string& body,
                                          const std::string& log_name);
    static bool _select_alive_process_from_pool(const std::vector<ProcessPtr>& pool,
                                                ProcessPtr* process);

    std::unordered_map<PythonVersion, std::shared_ptr<VersionedProcessPool>> _process_pools;
    // Protects the version -> pool handle map only. Per-version process operations are guarded
    // by VersionedProcessPool::mutex.
    std::mutex _pools_mutex;
    // Health check background thread
    std::unique_ptr<std::thread> _health_check_thread;
    std::atomic<bool> _shutdown_flag {false};
    std::condition_variable _health_check_cv;
    std::mutex _health_check_mutex;
    MemTracker _mem_tracker {"PythonUDFProcesses"};
};

} // namespace doris
