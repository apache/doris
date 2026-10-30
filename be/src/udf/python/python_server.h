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
#include <set>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "runtime/memory/mem_tracker.h"
#include "udf/python/python_udf_meta.h"
#include "udf/python/python_udf_runtime.h"

namespace doris {
struct UserFunctionCacheEntry;
class PythonUDFClient;
class PythonUDAFClient;
class PythonUDTFClient;

struct PythonProcessRegistry {
    void add(const ProcessPtr& process);
    std::vector<ProcessPtr> snapshot();

private:
    std::mutex mutex;
    std::vector<std::weak_ptr<PythonUDFProcess>> processes;
};

class PythonFunctionRuntime : public std::enable_shared_from_this<PythonFunctionRuntime> {
public:
    PythonFunctionRuntime(const PythonUDFMeta& meta, PythonVersion version,
                          std::shared_ptr<PythonProcessRegistry> process_registry,
                          std::shared_ptr<UserFunctionCacheEntry> package_cache_entry);

    template <typename T>
    Status get_client(const PythonUDFMeta& meta, std::shared_ptr<T>* client,
                      const std::shared_ptr<arrow::Schema>& data_schema = nullptr);

    void release_client();
    Status check_package() const;
    void reclaim_if_idle(std::chrono::steady_clock::time_point now);
    int repair();

    // Stop acquisition and publication; the caller releases processes outside its locks.
    std::vector<ProcessPtr> stop();

private:
    Status acquire_client();
    enum class PoolState { UNINITIALIZED, INITIALIZING, INITIALIZED, STOPPED };

    Status _ensure_pool_initialized();
    void _start_pool_initialization(std::chrono::milliseconds process_pool_init_timeout);
    bool _publish_process(ProcessPtr process, size_t index, uint64_t generation);
    Status _get_process(ProcessPtr* process);
    bool _select_alive_process_from_pool(ProcessPtr* process);
    void _start_pool_repair();
    int _repair_process_pool(uint64_t generation);

#ifdef BE_TEST
    static constexpr std::chrono::milliseconds PROCESS_POOL_INIT_TIMEOUT {1000};
    static constexpr std::chrono::milliseconds PROCESS_REPAIR_WAIT_TIMEOUT {200};
#else
    // Keep initialization below FE's default 30s send-fragments RPC timeout.
    static constexpr std::chrono::milliseconds PROCESS_POOL_INIT_TIMEOUT {20000};
    static constexpr std::chrono::milliseconds PROCESS_REPAIR_WAIT_TIMEOUT {1000};
#endif

    PythonVersion version;
    std::shared_ptr<PythonProcessRegistry> process_registry;
    int64_t function_id = 0;
    int32_t pool_size = 3;
    int64_t auto_suspend = PythonUDFMeta::DEFAULT_AUTO_SUSPEND_SECONDS;
    size_t active_clients = 0;
    std::chrono::steady_clock::time_point idle_since = std::chrono::steady_clock::now();
    std::shared_ptr<UserFunctionCacheEntry> package_cache_entry;
    std::mutex mutex;
    std::condition_variable cv;
    std::vector<ProcessPtr> processes;
    PoolState state = PoolState::UNINITIALIZED;
    bool has_available_process = false;
    bool repairing = false;
    // Initialization workers from before idle suspension cannot publish into a reopened pool.
    uint64_t pool_generation = 0;
};

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

    Result<std::shared_ptr<PythonFunctionRuntime>> get_runtime(
            const PythonUDFMeta& meta, const PythonVersion& version,
            std::shared_ptr<UserFunctionCacheEntry> package_cache_entry);

    std::set<int64_t> collect_pyudf_ids();

    // Remove the pool. Existing clients retain their processes until they release them.
    void drop_function(int64_t function_id);

    void shutdown();

private:
    /**
     * Start health check background thread when the first function pool is created
     * Thread periodically reclaims pools, checks process health and refreshes memory stats.
     */
    void _start_health_check_thread();

    // Reclaim pools after auto_suspend seconds without a client (-1 disables idle reclamation).
    void _reclaim_idle_pools(std::chrono::steady_clock::time_point now);

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

    std::vector<std::shared_ptr<PythonFunctionRuntime>> _snapshot_process_pools();

    // Weak references keep dropped-but-active processes visible without keeping them alive.
    std::shared_ptr<PythonProcessRegistry> _process_registry =
            std::make_shared<PythonProcessRegistry>();

    // function_id -> process_pool
    std::unordered_map<int64_t, std::shared_ptr<PythonFunctionRuntime>> _process_pools;
    // Protects the function id -> pool map. DROP also stops the pool under this lock;
    // when taking both locks, acquire this before PythonFunctionRuntime::mutex.
    std::mutex _pools_mutex;
    // Health check background thread
    std::unique_ptr<std::thread> _health_check_thread;
    std::atomic<bool> _shutdown_flag {false};
    std::condition_variable _health_check_cv;
    std::mutex _health_check_mutex;
    MemTracker _mem_tracker {"PythonUDFProcesses"};
};

extern template Status PythonServerManager::get_client<PythonUDFClient>(
        const PythonUDFMeta&, const PythonVersion&, std::shared_ptr<PythonUDFClient>*,
        const std::shared_ptr<arrow::Schema>&, std::shared_ptr<UserFunctionCacheEntry>);
extern template Status PythonServerManager::get_client<PythonUDAFClient>(
        const PythonUDFMeta&, const PythonVersion&, std::shared_ptr<PythonUDAFClient>*,
        const std::shared_ptr<arrow::Schema>&, std::shared_ptr<UserFunctionCacheEntry>);
extern template Status PythonServerManager::get_client<PythonUDTFClient>(
        const PythonUDFMeta&, const PythonVersion&, std::shared_ptr<PythonUDTFClient>*,
        const std::shared_ptr<arrow::Schema>&, std::shared_ptr<UserFunctionCacheEntry>);
extern template Status PythonFunctionRuntime::get_client<PythonUDFClient>(
        const PythonUDFMeta&, std::shared_ptr<PythonUDFClient>*,
        const std::shared_ptr<arrow::Schema>&);
extern template Status PythonFunctionRuntime::get_client<PythonUDAFClient>(
        const PythonUDFMeta&, std::shared_ptr<PythonUDAFClient>*,
        const std::shared_ptr<arrow::Schema>&);
extern template Status PythonFunctionRuntime::get_client<PythonUDTFClient>(
        const PythonUDFMeta&, std::shared_ptr<PythonUDTFClient>*,
        const std::shared_ptr<arrow::Schema>&);

} // namespace doris
