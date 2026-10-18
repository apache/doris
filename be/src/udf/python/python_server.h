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
#include <unordered_set>
#include <vector>

#include "common/status.h"
#include "runtime/memory/mem_tracker.h"
#include "udf/python/python_udf_meta.h"
#include "udf/python/python_udf_runtime.h"

namespace doris {
struct UserFunctionCacheEntry;
class PythonUDFPoolLease;
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
                      const std::shared_ptr<arrow::Schema>& data_schema = nullptr,
                      std::shared_ptr<UserFunctionCacheEntry> package_cache_entry = nullptr);

    static Status fork(const PythonVersion& version, ProcessPtr* process);

    // Remove this function's pool. Existing clients keep their processes and zip alive.
    void drop_function(int64_t function_id);

    void shutdown();

#ifdef BE_TEST
    // For unit testing only.
    void check_and_recreate_processes_for_test() { _check_and_recreate_processes(); }

    void set_process_pool_for_test(const PythonUDFMeta& meta, const PythonVersion& version,
                                   std::vector<ProcessPtr> processes, bool initialized = true);

    std::vector<ProcessPtr> process_pool_snapshot_for_test(int64_t function_id);

    bool process_pool_is_initialized_for_test(int64_t function_id);

    bool wait_for_process_pool_initialized_for_test(int64_t function_id,
                                                    std::chrono::milliseconds timeout);

    bool wait_for_process_pool_initialization_finished_for_test(int64_t function_id,
                                                                std::chrono::milliseconds timeout);

#endif

private:
    friend class PythonUDFPoolLease;

    enum class PoolState {
        UNINITIALIZED,
        INITIALIZING,
        // `Initialized` means:
        // 1. All process slots have attempted initialization.
        // 2. At least one live process is available for requests.
        INITIALIZED,
        // Prevent the pool from being incorrectly reused after `shutdown()`
        STOPPED,
    };

    // Includes processes held by clients after DROP, without extending their lifetime.
    struct ProcessRegistry {
        void add(const ProcessPtr& process);
        std::vector<ProcessPtr> snapshot();

        std::mutex mutex;
        std::vector<std::weak_ptr<PythonUDFProcess>> processes;
    };

    struct FunctionProcessPool {
        // Interpreter information needed to replace dead processes in this function pool.
        PythonVersion version;
        // Detached fork/repair workers can outlive the manager.
        std::shared_ptr<ProcessRegistry> process_registry;
        int64_t function_id = 0;
        int32_t pool_size = 3;
        int64_t auto_suspend = -1;
        size_t active_clients = 0;
        std::chrono::steady_clock::time_point idle_since = std::chrono::steady_clock::now();
        std::shared_ptr<UserFunctionCacheEntry> package_cache_entry;
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
     * Lazily initialize and return the process pool for specific function.
     */
    Result<std::shared_ptr<FunctionProcessPool>> _ensure_pool_initialized(
            const PythonUDFMeta& meta, const PythonVersion& version,
            std::shared_ptr<UserFunctionCacheEntry> package_cache_entry = nullptr,
            std::shared_ptr<PythonUDFPoolLease>* lease = nullptr);

    /**
     * Pick an available process from specific pool, recreating one on demand if needed.
     */
    static void _start_pool_initialization(
            const std::shared_ptr<FunctionProcessPool>& function_pool, const PythonVersion& version,
            std::chrono::milliseconds process_pool_init_timeout);

    static void _start_pool_repair(const PythonVersion& version,
                                   const std::shared_ptr<FunctionProcessPool>& function_pool);

    Status _get_process(const PythonVersion& version,
                        const std::shared_ptr<FunctionProcessPool>& function_pool,
                        ProcessPtr* process);

    /**
     * Start health check background thread when the first function pool is created
     * Thread periodically checks process health and refreshes memory stats
     */
    void _start_health_check_thread();

    void _reclaim_idle_pools(std::chrono::steady_clock::time_point now);

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
                                    const std::shared_ptr<FunctionProcessPool>& function_pool);

    /**
     * Read resident set size (RSS) for a single process from /proc/{pid}/statm
     */
    Status _read_process_memory(pid_t pid, size_t* rss_bytes);

    /**
     * Refresh memory statistics for all Python processes
     */
    void _refresh_memory_stats();

    Result<std::shared_ptr<FunctionProcessPool>> _get_or_create_process_pool(
            const PythonUDFMeta& meta, const PythonVersion& version,
            std::shared_ptr<UserFunctionCacheEntry> package_cache_entry,
            std::shared_ptr<PythonUDFPoolLease>* lease = nullptr);
    std::vector<std::shared_ptr<FunctionProcessPool>> _snapshot_process_pools();
    static bool _select_alive_process_from_pool(const std::vector<ProcessPtr>& pool,
                                                ProcessPtr* process);

    // Weak references keep dropped-but-active processes visible without keeping them alive.
    std::shared_ptr<ProcessRegistry> _process_registry = std::make_shared<ProcessRegistry>();

    // function_id -> process_pool
    std::unordered_map<int64_t, std::shared_ptr<FunctionProcessPool>> _process_pools;
    // Catalog ids are never reused. Reject late opens/aggregate-place creation after DROP,
    // including inline functions that have no zip cache entry carrying a deletion flag.
    std::unordered_set<int64_t> _dropped_function_ids;
    // Protects the function id -> pool map. DROP also stops the pool under this lock;
    // when taking both locks, acquire this before FunctionProcessPool::mutex.
    std::mutex _pools_mutex;
    // Health check background thread
    std::unique_ptr<std::thread> _health_check_thread;
    std::atomic<bool> _shutdown_flag {false};
    std::condition_variable _health_check_cv;
    std::mutex _health_check_mutex;
    MemTracker _mem_tracker {"PythonUDFProcesses"};
};

} // namespace doris
