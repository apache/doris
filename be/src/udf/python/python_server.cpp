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

#include "udf/python/python_server.h"

#include <arrow/type_fwd.h>
#include <butil/fd_utility.h>
#include <dirent.h>
#include <fmt/core.h>
#include <sys/poll.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <fstream>
#include <iterator>
#include <thread>

#include "arrow/flight/client.h"
#include "common/config.h"
#include "common/status.h"
#include "cpp/sync_point.h"
#include "runtime/thread_context.h"
#include "runtime/user_function_cache.h"
#include "udf/python/python_udaf_client.h"
#include "udf/python/python_udf_client.h"
#include "udf/python/python_udtf_client.h"
#include "util/cpu_info.h"
#include "util/defer_op.h"

namespace doris {

PythonFunctionRuntime::PythonFunctionRuntime(
        const PythonUDFMeta& meta, PythonVersion version,
        std::shared_ptr<PythonProcessRegistry> process_registry,
        std::shared_ptr<UserFunctionCacheEntry> package_cache_entry)
        : version(std::move(version)),
          process_registry(std::move(process_registry)),
          function_id(meta.id),
          pool_size(std::min(meta.pool_size, CpuInfo::num_cores())),
          auto_suspend(meta.auto_suspend),
          package_cache_entry(std::move(package_cache_entry)) {}

Status PythonFunctionRuntime::acquire_client() {
    std::scoped_lock lock(mutex);
    // A GROUP BY can request another client after DROP retired this runtime.
    if (state == PoolState::STOPPED) {
        return Status::Error<ErrorCode::SERVICE_UNAVAILABLE>(
                "Python UDF function {} is being deleted", function_id);
    }
    ++active_clients;
    return Status::OK();
}

void PythonFunctionRuntime::release_client() {
    std::scoped_lock lock(mutex);
    DCHECK_GT(active_clients, 0);
    if (--active_clients == 0) {
        idle_since = std::chrono::steady_clock::now();
    }
}

void PythonProcessRegistry::add(const ProcessPtr& process) {
    std::scoped_lock lock(mutex);
    processes.emplace_back(process);
}

std::vector<ProcessPtr> PythonProcessRegistry::snapshot() {
    std::vector<ProcessPtr> snapshot;
    std::scoped_lock lock(mutex);
    std::erase_if(processes, [&snapshot](const auto& weak_process) {
        if (auto process = weak_process.lock()) {
            snapshot.emplace_back(std::move(process));
            return false;
        }
        return true;
    });
    return snapshot;
}

Result<std::shared_ptr<PythonFunctionRuntime>> PythonServerManager::get_runtime(
        const PythonUDFMeta& meta, const PythonVersion& version,
        std::shared_ptr<UserFunctionCacheEntry> package_cache_entry) {
    std::shared_ptr<PythonFunctionRuntime> function_pool;
    {
        std::scoped_lock lock(_pools_mutex);
        // Reject acquisition once manager shutdown has started.
        if (_shutdown_flag.load(std::memory_order_acquire)) {
            return ResultError(Status::Error<ErrorCode::SERVICE_UNAVAILABLE>(
                    "Python server manager is shutting down"));
        }
        if (meta.pool_size <= 0) {
            return ResultError(
                    Status::InvalidArgument("Python UDF pool_size must be a positive integer"));
        }
        if (meta.auto_suspend < -1) {
            return ResultError(Status::InvalidArgument(
                    "Python UDF auto_suspend must be -1 or a non-negative integer"));
        }
        auto& pool = _process_pools[meta.id];
        if (!pool) {
            pool = std::make_shared<PythonFunctionRuntime>(meta, version, _process_registry,
                                                           std::move(package_cache_entry));
            _start_health_check_thread();
        }
        function_pool = pool;
    }
    // Acquire: get entry -> register runtime -> check entry -> initialize/get process.
    // DROP: add tombstone -> remove cache entry -> stop and remove pool.
    auto status = function_pool->check_package();
    if (!status.ok()) {
        drop_function(meta.id);
        return ResultError(std::move(status));
    }
    return function_pool;
}

std::set<int64_t> PythonServerManager::collect_pyudf_ids() {
    std::set<int64_t> ids;
    std::scoped_lock lock(_pools_mutex);
    for (const auto& [id, runtime] : _process_pools) {
        ids.insert(id);
    }
    return ids;
}

Status PythonFunctionRuntime::check_package() const {
#ifdef BE_TEST
    TEST_SYNC_POINT_CALLBACK("PythonFunctionRuntime::check_package:before_check");
#endif
    // DROP may run between entry acquisition and pool registration.
    if (package_cache_entry && UserFunctionCache::is_pending_deletion(*package_cache_entry)) {
        return Status::Error<ErrorCode::SERVICE_UNAVAILABLE>(
                "Python UDF function {} is being deleted", function_id);
    }
    return Status::OK();
}

std::vector<std::shared_ptr<PythonFunctionRuntime>> PythonServerManager::_snapshot_process_pools() {
    std::scoped_lock lock(_pools_mutex);
    std::vector<std::shared_ptr<PythonFunctionRuntime>> snapshot;
    snapshot.reserve(_process_pools.size());
    for (const auto& [function_id, pool] : _process_pools) {
        snapshot.emplace_back(pool);
    }
    return snapshot;
}

bool PythonFunctionRuntime::_select_alive_process_from_pool(ProcessPtr* process) {
    const auto& pool = processes;
    auto alive_iter = std::ranges::min_element(pool, [](const ProcessPtr& a, const ProcessPtr& b) {
        const bool a_alive = a && a->is_alive();
        const bool b_alive = b && b->is_alive();
        if (a_alive != b_alive) {
            return a_alive > b_alive;
        }
        return a.use_count() < b.use_count();
    });
    if (alive_iter == pool.end() || !*alive_iter || !(*alive_iter)->is_alive()) {
        return false;
    }
    *process = *alive_iter;
    return true;
}

template <typename ClientType>
Status PythonServerManager::get_client(
        const PythonUDFMeta& func_meta, const PythonVersion& version,
        std::shared_ptr<ClientType>* client, const std::shared_ptr<arrow::Schema>& data_schema,
        std::shared_ptr<UserFunctionCacheEntry> package_cache_entry) {
    auto runtime = DORIS_TRY(get_runtime(func_meta, version, std::move(package_cache_entry)));
    return runtime->get_client(func_meta, client, data_schema);
}

template <typename ClientType>
Status PythonFunctionRuntime::get_client(const PythonUDFMeta& func_meta,
                                         std::shared_ptr<ClientType>* client,
                                         const std::shared_ptr<arrow::Schema>& data_schema) {
    RETURN_IF_ERROR(acquire_client());
    bool acquired = false;
    Defer release_on_failure([&]() {
        if (!acquired) {
            release_client();
        }
    });
    RETURN_IF_ERROR(_ensure_pool_initialized());

    ProcessPtr process;
    RETURN_IF_ERROR(_get_process(&process));

    if constexpr (std::is_same_v<ClientType, PythonUDAFClient>) {
        RETURN_IF_ERROR(ClientType::create(func_meta, std::move(process), data_schema, client));
    } else {
        RETURN_IF_ERROR(ClientType::create(func_meta, std::move(process), client));
    }

    (*client)->_runtime = shared_from_this();
    acquired = true;
    return Status::OK();
}

void PythonFunctionRuntime::_start_pool_initialization(
        std::chrono::milliseconds process_pool_init_timeout) {
    // Called with the pool lock held; workers publish only after the caller starts waiting.
    const auto generation = ++pool_generation;
    state = PoolState::INITIALIZING;
    has_available_process = false;
    processes.resize(pool_size);
    auto init_finished_count = std::make_shared<std::atomic<int>>(0);
#ifdef BE_TEST
    TEST_SYNC_POINT_CALLBACK("PythonFunctionRuntime::_ensure_pool_initialized:generation_started",
                             &version, init_finished_count.get());
#endif

    LOG(INFO) << "Initializing Python process pool for function " << function_id << " with "
              << pool_size << " processes, runtime=" << version.to_string();

    auto self = shared_from_this();
    std::thread([self, init_finished_count, process_pool_init_timeout, generation]() {
        SCOPED_INIT_THREAD_CONTEXT();
        std::unique_lock<std::mutex> lock(self->mutex);
        self->cv.wait_for(
                lock, process_pool_init_timeout, [&self, init_finished_count, generation]() {
                    return self->state != PoolState::INITIALIZING ||
                           self->pool_generation != generation ||
                           init_finished_count->load(std::memory_order_acquire) >= self->pool_size;
                });
        if (self->state == PoolState::INITIALIZING && self->pool_generation == generation) {
            self->state =
                    self->has_available_process ? PoolState::INITIALIZED : PoolState::UNINITIALIZED;
        }
        self->cv.notify_all();
    }).detach();

    for (int i = 0; i < pool_size; ++i) {
        std::thread([self, i, init_finished_count, generation]() {
            SCOPED_INIT_THREAD_CONTEXT();
            ProcessPtr process;
            Status status = PythonUDFProcess::create(self->version, &process);
            if (status.ok()) {
                self->_publish_process(std::move(process), i, generation);
            } else [[unlikely]] {
                LOG(WARNING) << "Failed to create Python process " << (i + 1) << "/"
                             << self->pool_size << " for version " << self->version.to_string()
                             << ": " << status.to_string();
            }
            init_finished_count->fetch_add(1, std::memory_order_acq_rel);
            self->cv.notify_all();
#ifdef BE_TEST
            TEST_SYNC_POINT_CALLBACK(
                    "PythonFunctionRuntime::_ensure_pool_initialized:init_worker_finished",
                    &self->version, init_finished_count.get());
#endif
        }).detach();
    }
}

bool PythonFunctionRuntime::_publish_process(ProcessPtr process, size_t index,
                                             uint64_t generation) {
    {
        std::scoped_lock lock(mutex);
        // DROP and idle suspension invalidate workers; rejected processes are released after unlock.
        if (pool_generation != generation ||
            (state != PoolState::INITIALIZING && state != PoolState::INITIALIZED)) {
            return false;
        }
        DCHECK_LT(index, processes.size());
        // Initialization and repair can finish for the same slot in either order.
        if (processes[index] && processes[index]->is_alive()) {
            return false;
        }
        process_registry->add(process);
        process->set_package_cache_entry(package_cache_entry);
        processes[index] = std::move(process);
        has_available_process = true;
    }
    cv.notify_all();
    return true;
}

Status PythonFunctionRuntime::_ensure_pool_initialized() {
    auto process_pool_init_timeout = PROCESS_POOL_INIT_TIMEOUT;
#ifdef BE_TEST
    TEST_SYNC_POINT_CALLBACK(
            "PythonFunctionRuntime::_ensure_pool_initialized:process_pool_init_timeout",
            &process_pool_init_timeout);
#endif

    std::unique_lock<std::mutex> lock(mutex);
    if (state == PoolState::INITIALIZED) {
        return Status::OK();
    }

    if (state != PoolState::STOPPED) {
        if (state != PoolState::INITIALIZING) {
            _start_pool_initialization(process_pool_init_timeout);
        }

        // Requests can proceed when the first process is ready, before initialization finishes.
        cv.wait_for(lock, process_pool_init_timeout,
                    [this]() { return has_available_process || state != PoolState::INITIALIZING; });
        if (has_available_process) {
            return Status::OK();
        }
        cv.notify_all();
    }

    return Status::Error<ErrorCode::SERVICE_UNAVAILABLE>(
            "Failed to initialize Python process pool for version {}: no process became available "
            "within {} ms",
            version.to_string(), process_pool_init_timeout.count());
}

void PythonFunctionRuntime::_start_pool_repair() {
    // Called with the pool lock held so concurrent requests cannot schedule the same repair.
    repairing = true;
    const auto generation = pool_generation;
    // Repair is done in the background because fork can be slow. The current request still
    // waits briefly below so a transient all-dead pool can recover without failing.
    std::thread([self = shared_from_this(), generation]() {
        SCOPED_INIT_THREAD_CONTEXT();
        int recreated = self->_repair_process_pool(generation);
        if (recreated > 0) {
            LOG(INFO) << "Repaired Python process pool for version " << self->version.to_string()
                      << ": recreated=" << recreated;
        }
    }).detach();
}

Status PythonFunctionRuntime::_get_process(ProcessPtr* process) {
    {
        std::unique_lock<std::mutex> lock(mutex);

        if (state == PoolState::STOPPED) {
            has_available_process = false;
            return Status::Error<ErrorCode::SERVICE_UNAVAILABLE>(
                    "Python process pool has stopped for version {}", version.to_string());
        }

        if (_select_alive_process_from_pool(process)) [[likely]] {
            has_available_process = true;
            return Status::OK();
        }
        has_available_process = false;

        if (state == PoolState::INITIALIZING) {
            cv.wait_for(lock, PROCESS_REPAIR_WAIT_TIMEOUT, [this]() {
                return std::ranges::any_of(
                               processes, [](const ProcessPtr& p) { return p && p->is_alive(); }) ||
                       state != PoolState::INITIALIZING;
            });
            if (state == PoolState::STOPPED) {
                return Status::Error<ErrorCode::SERVICE_UNAVAILABLE>(
                        "Python process pool has stopped for version {}", version.to_string());
            }
            if (_select_alive_process_from_pool(process)) {
                has_available_process = true;
                return Status::OK();
            }
            has_available_process = false;
            return Status::Error<ErrorCode::SERVICE_UNAVAILABLE>(
                    "Python process pool is initializing but has no available process for version "
                    "{} after waiting {} ms",
                    version.to_string(), PROCESS_REPAIR_WAIT_TIMEOUT.count());
        }

        if (state != PoolState::INITIALIZED) {
            return Status::Error<ErrorCode::SERVICE_UNAVAILABLE>(
                    "Python process pool is not initialized for version {}", version.to_string());
        }

        if (!repairing) {
            _start_pool_repair();
        }

        // Keep the request recoverable in the common case where the Python runtime can fork
        // normally and only the existing pool entries died. The wait is short so a wedged fork path
        // still returns SERVICE_UNAVAILABLE promptly.
        cv.wait_for(lock, PROCESS_REPAIR_WAIT_TIMEOUT, [this]() {
            return std::ranges::any_of(processes,
                                       [](const ProcessPtr& p) { return p && p->is_alive(); }) ||
                   state == PoolState::STOPPED;
        });
        if (state == PoolState::STOPPED) {
            has_available_process = false;
            return Status::Error<ErrorCode::SERVICE_UNAVAILABLE>(
                    "Python process pool has stopped for version {}", version.to_string());
        }

        if (_select_alive_process_from_pool(process)) {
            has_available_process = true;
            return Status::OK();
        }
        has_available_process = false;
    }

    return Status::Error<ErrorCode::SERVICE_UNAVAILABLE>(
            "Python process pool has no available process for version {} after waiting repair for "
            "{} ms",
            version.to_string(), PROCESS_REPAIR_WAIT_TIMEOUT.count());
}

void PythonServerManager::_reclaim_idle_pools(std::chrono::steady_clock::time_point now) {
    for (const auto& pool : _snapshot_process_pools()) {
        pool->reclaim_if_idle(now);
    }
}

void PythonFunctionRuntime::reclaim_if_idle(std::chrono::steady_clock::time_point now) {
    std::vector<ProcessPtr> processes_to_shutdown;
    {
        std::scoped_lock lock(mutex);
        // An already suspended runtime has no processes left to reclaim.
        if (state == PoolState::STOPPED || auto_suspend < 0 ||
            (state == PoolState::UNINITIALIZED && processes.empty()) || active_clients != 0 ||
            std::chrono::duration_cast<std::chrono::seconds>(now - idle_since).count() <
                    auto_suspend) {
            return;
        }
        // Keep the query's runtime usable; invalidate workers from the suspended pool.
        ++pool_generation;
        state = PoolState::UNINITIALIZED;
        has_available_process = false;
        repairing = false;
        processes_to_shutdown.swap(processes);
    }
    cv.notify_all();
    for (const auto& process : processes_to_shutdown) {
        if (process) {
            process->shutdown();
        }
    }
}

void PythonServerManager::_start_health_check_thread() {
    std::scoped_lock lock(_health_check_mutex);
    if (_health_check_thread || _shutdown_flag.load(std::memory_order_acquire)) return;

    LOG(INFO) << "Starting Python process monitor";

    _health_check_thread = std::make_unique<std::thread>([this]() {
        while (!_shutdown_flag.load(std::memory_order_acquire)) {
            // Wait for interval or shutdown signal
            {
                std::unique_lock<std::mutex> lock(_health_check_mutex);
                _health_check_cv.wait_for(lock, std::chrono::seconds(30), [this]() {
                    return _shutdown_flag.load(std::memory_order_acquire);
                });
            }

            if (_shutdown_flag.load(std::memory_order_acquire)) {
                break;
            }

            _reclaim_idle_pools(std::chrono::steady_clock::now());
            _check_and_recreate_processes();
            _refresh_memory_stats();
        }

        LOG(INFO) << "Python process health check thread exiting";
    });
}

void PythonServerManager::_check_and_recreate_processes() {
    int total_recreated = 0;
    for (const auto& function_pool : _snapshot_process_pools()) {
        total_recreated += function_pool->repair();
    }

    if (total_recreated > 0) {
        LOG(INFO) << "Health check completed: recreated=" << total_recreated;
    }
}

int PythonFunctionRuntime::repair() {
    uint64_t generation;
    {
        std::scoped_lock lock(mutex);
        if (state != PoolState::INITIALIZED || repairing) {
            return 0;
        }
        // Foreground requests and health checks must not repair the same slots concurrently.
        repairing = true;
        generation = pool_generation;
    }
    return _repair_process_pool(generation);
}

int PythonFunctionRuntime::_repair_process_pool(uint64_t generation) {
    Defer finish_repair([this, generation]() {
        {
            std::scoped_lock lock(mutex);
            if (pool_generation == generation) {
                repairing = false;
            }
        }
        cv.notify_all();
    });
    std::vector<size_t> died_process_indices;
    {
        std::scoped_lock lock(mutex);
        if (state != PoolState::INITIALIZED || pool_generation != generation) {
            return 0;
        }

        auto& pool = processes;
        died_process_indices.reserve(pool.size());
        for (size_t i = 0; i < pool.size(); ++i) {
            const auto& process = pool[i];
            if (!process || !process->is_alive()) {
                died_process_indices.push_back(i);
            }
        }
    }

    if (died_process_indices.empty()) [[likely]] {
        return 0;
    }

    int recreated = 0;
    for (size_t index : died_process_indices) {
        {
            std::scoped_lock lock(mutex);
            if (state != PoolState::INITIALIZED || pool_generation != generation) {
                break;
            }
        }

        ProcessPtr new_process;
        Status status = PythonUDFProcess::create(version, &new_process);
        if (status.ok()) {
            if (_publish_process(std::move(new_process), index, generation)) {
                ++recreated;
            }
        } else {
            LOG(ERROR) << "Failed to recreate Python process for version " << version.to_string()
                       << ": " << status.to_string();
        }
    }

    {
        std::scoped_lock lock(mutex);
        auto& pool = processes;

        if (pool_generation == generation && state == PoolState::INITIALIZED) {
            // Keep empty/dead slots instead of shrinking the vector. Init workers are detached and
            // publish by original slot index; shrinking here would make a late successful init look
            // out-of-range and discard a usable process.
            has_available_process = std::ranges::any_of(
                    pool, [](const ProcessPtr& process) { return process && process->is_alive(); });
        }
    }
    return recreated;
}

std::vector<ProcessPtr> PythonFunctionRuntime::stop() {
    std::vector<ProcessPtr> detached_processes;
    {
        std::scoped_lock lock(mutex);
        state = PoolState::STOPPED;
        has_available_process = false;
        repairing = false;
        detached_processes.swap(processes);
    }
    cv.notify_all();
    return detached_processes;
}

void PythonServerManager::shutdown() {
    std::vector<std::shared_ptr<PythonFunctionRuntime>> pools_to_shutdown;
    {
        std::scoped_lock lock(_pools_mutex);
        _shutdown_flag.store(true, std::memory_order_release);
        pools_to_shutdown.reserve(_process_pools.size());
        for (auto& [function_id, function_pool] : _process_pools) {
            pools_to_shutdown.emplace_back(std::move(function_pool));
        }
        _process_pools.clear();
    }

    std::vector<ProcessPtr> retained_processes;
    for (const auto& function_pool : pools_to_shutdown) {
        auto processes = function_pool->stop();
        retained_processes.insert(retained_processes.end(),
                                  std::make_move_iterator(processes.begin()),
                                  std::make_move_iterator(processes.end()));
    }

    std::unique_ptr<std::thread> health_check_thread;
    {
        std::scoped_lock lock(_health_check_mutex);
        health_check_thread = std::move(_health_check_thread);
    }
    _health_check_cv.notify_one();
    if (health_check_thread && health_check_thread->joinable()) {
        health_check_thread->join();
    }

    // Include processes retained by clients after DROP; shut down outside all pool locks.
    auto processes_to_shutdown = _process_registry->snapshot();
    retained_processes.clear();
    for (const auto& process : processes_to_shutdown) {
        process->shutdown();
    }
}

Status PythonServerManager::_read_process_memory(pid_t pid, size_t* rss_bytes) {
    // Read from /proc/{pid}/statm
    // Format: size resident shared text lib data dt
    std::string statm_path = fmt::format("/proc/{}/statm", pid);
    std::ifstream statm_file(statm_path);

    if (!statm_file.is_open()) {
        return Status::InternalError("Cannot open {}", statm_path);
    }

    size_t size_pages = 0, rss_pages = 0;
    // we only care about RSS, read and ignore the total size field
    statm_file >> size_pages >> rss_pages;

    if (statm_file.fail()) {
        return Status::InternalError("Failed to read {}", statm_path);
    }

    // Convert pages to bytes
    long page_size = sysconf(_SC_PAGESIZE);
    *rss_bytes = rss_pages * page_size;

    return Status::OK();
}

void PythonServerManager::_refresh_memory_stats() {
    int64_t total_rss = 0;

    // DROP removes scheduling pools immediately, but active clients can retain their processes.
    for (const auto& process : _process_registry->snapshot()) {
        if (!process->is_alive()) {
            continue;
        }

        size_t rss_bytes = 0;
        Status s = _read_process_memory(process->get_child_pid(), &rss_bytes);
        if (s.ok()) {
            total_rss += rss_bytes;
        } else [[unlikely]] {
            LOG(WARNING) << "Failed to read memory info for Python process (pid="
                         << process->get_child_pid() << "): " << s.to_string();
        }
    }
    _mem_tracker.set_consumption(total_rss);
    LOG(INFO) << _mem_tracker.log_usage();

    if (config::python_udf_processes_memory_limit_bytes > 0 &&
        total_rss > config::python_udf_processes_memory_limit_bytes) {
        LOG(WARNING) << "Python UDF process memory usage exceeds limit: rss_bytes=" << total_rss
                     << ", limit_bytes=" << config::python_udf_processes_memory_limit_bytes;
    }
}

void PythonServerManager::drop_function(int64_t function_id) {
    std::shared_ptr<PythonFunctionRuntime> pool;
    std::vector<ProcessPtr> processes;
    {
        std::scoped_lock map_lock(_pools_mutex);
        auto iter = _process_pools.find(function_id);
        if (iter == _process_pools.end()) {
            return;
        }
        pool = std::move(iter->second);
        // Stop init/repair -> detach processes -> erase pool -> unlock -> release references.
        processes = pool->stop();
        _process_pools.erase(iter);
    }
}

// Explicit template instantiation for UDF, UDAF and UDTF clients
template Status PythonServerManager::get_client<PythonUDFClient>(
        const PythonUDFMeta&, const PythonVersion&, std::shared_ptr<PythonUDFClient>*,
        const std::shared_ptr<arrow::Schema>&, std::shared_ptr<UserFunctionCacheEntry>);

template Status PythonServerManager::get_client<PythonUDAFClient>(
        const PythonUDFMeta&, const PythonVersion&, std::shared_ptr<PythonUDAFClient>*,
        const std::shared_ptr<arrow::Schema>&, std::shared_ptr<UserFunctionCacheEntry>);

template Status PythonServerManager::get_client<PythonUDTFClient>(
        const PythonUDFMeta&, const PythonVersion&, std::shared_ptr<PythonUDTFClient>*,
        const std::shared_ptr<arrow::Schema>&, std::shared_ptr<UserFunctionCacheEntry>);

template Status PythonFunctionRuntime::get_client<PythonUDFClient>(
        const PythonUDFMeta&, std::shared_ptr<PythonUDFClient>*,
        const std::shared_ptr<arrow::Schema>&);
template Status PythonFunctionRuntime::get_client<PythonUDAFClient>(
        const PythonUDFMeta&, std::shared_ptr<PythonUDAFClient>*,
        const std::shared_ptr<arrow::Schema>&);
template Status PythonFunctionRuntime::get_client<PythonUDTFClient>(
        const PythonUDFMeta&, std::shared_ptr<PythonUDTFClient>*,
        const std::shared_ptr<arrow::Schema>&);

} // namespace doris
