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
#include <signal.h>
#include <sys/poll.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <boost/asio.hpp>
#include <boost/process.hpp>
#include <chrono>
#include <fstream>
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

// A client holds this from pool acquisition through stream/state cleanup. The pool may outlive
// its manager after DROP; releasing a lease must therefore only access the pool itself.
class PythonUDFPoolLease {
public:
    explicit PythonUDFPoolLease(std::shared_ptr<PythonServerManager::FunctionProcessPool> pool)
            : _pool(std::move(pool)) {}

    ~PythonUDFPoolLease() {
        std::scoped_lock lock(_pool->mutex);
        DCHECK_GT(_pool->active_clients, 0);
        if (--_pool->active_clients == 0) {
            _pool->idle_since = std::chrono::steady_clock::now();
        }
    }

private:
    std::shared_ptr<PythonServerManager::FunctionProcessPool> _pool;
};

void PythonServerManager::ProcessRegistry::add(const ProcessPtr& process) {
    std::scoped_lock lock(mutex);
    processes.emplace_back(process);
}

std::vector<ProcessPtr> PythonServerManager::ProcessRegistry::snapshot() {
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

Result<std::shared_ptr<PythonServerManager::FunctionProcessPool>>
PythonServerManager::_get_or_create_process_pool(
        const PythonUDFMeta& meta, const PythonVersion& version,
        std::shared_ptr<UserFunctionCacheEntry> package_cache_entry,
        std::shared_ptr<PythonUDFPoolLease>* lease) {
    std::scoped_lock lock(_pools_mutex);
    // shutdown() owns the manager lifecycle. Once it starts, creating a new pool would let detached
    // init workers publish Python processes that the manager no longer tracks.
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
        pool = std::make_shared<FunctionProcessPool>();
        pool->version = version;
        pool->process_registry = _process_registry;
        pool->function_id = meta.id;
        // The same function may run on BEs with different CPU limits.
        pool->pool_size = std::min(meta.pool_size, CpuInfo::num_cores());
        pool->auto_suspend = meta.auto_suspend;
        pool->package_cache_entry = std::move(package_cache_entry);
        // The first process can serve a client before the remaining slots finish initialization.
        _start_health_check_thread();
    }
    if (lease) {
        auto acquired = std::make_shared<PythonUDFPoolLease>(pool);
        std::scoped_lock pool_lock(pool->mutex);
        ++pool->active_clients;
        *lease = std::move(acquired);
    }
    return pool;
}

std::vector<std::shared_ptr<PythonServerManager::FunctionProcessPool>>
PythonServerManager::_snapshot_process_pools() {
    std::scoped_lock lock(_pools_mutex);
    std::vector<std::shared_ptr<FunctionProcessPool>> snapshot;
    snapshot.reserve(_process_pools.size());
    for (const auto& [function_id, pool] : _process_pools) {
        snapshot.emplace_back(pool);
    }
    return snapshot;
}

#ifdef BE_TEST
void PythonServerManager::set_process_pool_for_test(const PythonUDFMeta& meta,
                                                    const PythonVersion& version,
                                                    std::vector<ProcessPtr> processes,
                                                    bool initialized) {
    auto function_pool = _get_or_create_process_pool(meta, version, nullptr).value();
    std::scoped_lock lock(function_pool->mutex);
    for (const auto& process : processes) {
        if (process) {
            function_pool->process_registry->add(process);
        }
    }
    function_pool->processes = std::move(processes);
    function_pool->state = initialized ? PoolState::INITIALIZED : PoolState::UNINITIALIZED;
    function_pool->has_available_process =
            std::any_of(function_pool->processes.begin(), function_pool->processes.end(),
                        [](const ProcessPtr& process) { return process && process->is_alive(); });
}

std::vector<ProcessPtr> PythonServerManager::process_pool_snapshot_for_test(int64_t function_id) {
    std::scoped_lock map_lock(_pools_mutex);
    auto iter = _process_pools.find(function_id);
    if (iter == _process_pools.end()) {
        return {};
    }
    std::scoped_lock lock(iter->second->mutex);
    return iter->second->processes;
}

bool PythonServerManager::process_pool_is_initialized_for_test(int64_t function_id) {
    return wait_for_process_pool_initialized_for_test(function_id, std::chrono::milliseconds(0));
}

bool PythonServerManager::wait_for_process_pool_initialized_for_test(
        int64_t function_id, std::chrono::milliseconds timeout) {
    std::shared_ptr<FunctionProcessPool> pool;
    {
        std::scoped_lock map_lock(_pools_mutex);
        pool = _process_pools.at(function_id);
    }
    std::unique_lock<std::mutex> lock(pool->mutex);
    return pool->cv.wait_for(lock, timeout,
                             [&]() { return pool->state == PoolState::INITIALIZED; });
}

bool PythonServerManager::wait_for_process_pool_initialization_finished_for_test(
        int64_t function_id, std::chrono::milliseconds timeout) {
    std::shared_ptr<FunctionProcessPool> pool;
    {
        std::scoped_lock map_lock(_pools_mutex);
        pool = _process_pools.at(function_id);
    }
    std::unique_lock<std::mutex> lock(pool->mutex);
    return pool->cv.wait_for(lock, timeout,
                             [&]() { return pool->state != PoolState::INITIALIZING; });
}
#endif

bool PythonServerManager::_select_alive_process_from_pool(const std::vector<ProcessPtr>& pool,
                                                          ProcessPtr* process) {
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
    std::shared_ptr<PythonUDFPoolLease> lease;
    std::shared_ptr<FunctionProcessPool> function_pool = DORIS_TRY(
            _ensure_pool_initialized(func_meta, version, std::move(package_cache_entry), &lease));

    ProcessPtr process;
    RETURN_IF_ERROR(_get_process(version, function_pool, &process));

    if constexpr (std::is_same_v<ClientType, PythonUDAFClient>) {
        RETURN_IF_ERROR(ClientType::create(func_meta, std::move(process), data_schema, client));
    } else {
        RETURN_IF_ERROR(ClientType::create(func_meta, std::move(process), client));
    }

    (*client)->_pool_lease = std::move(lease);
    return Status::OK();
}

void PythonServerManager::_start_pool_initialization(
        const std::shared_ptr<FunctionProcessPool>& function_pool, const PythonVersion& version,
        std::chrono::milliseconds process_pool_init_timeout) {
    // Called with the pool lock held; workers publish only after the caller starts waiting.
    const int max_pool_size = function_pool->pool_size;
    function_pool->state = PoolState::INITIALIZING;
    function_pool->has_available_process = false;
    function_pool->processes.resize(max_pool_size);
    auto init_finished_count = std::make_shared<std::atomic<int>>(0);
    TEST_SYNC_POINT_CALLBACK("PythonServerManager::_ensure_pool_initialized:generation_started",
                             &version, init_finished_count.get());

    LOG(INFO) << "Initializing Python process pool for function " << function_pool->function_id
              << " with " << max_pool_size << " processes, runtime=" << version.to_string();

    std::thread([function_pool, init_finished_count, max_pool_size, process_pool_init_timeout]() {
        SCOPED_INIT_THREAD_CONTEXT();
        std::unique_lock<std::mutex> lock(function_pool->mutex);
        function_pool->cv.wait_for(
                lock, process_pool_init_timeout,
                [&function_pool, init_finished_count, max_pool_size]() {
                    return function_pool->state != PoolState::INITIALIZING ||
                           init_finished_count->load(std::memory_order_acquire) >= max_pool_size;
                });
        if (function_pool->state == PoolState::INITIALIZING) {
            if (function_pool->has_available_process) {
                function_pool->state = PoolState::INITIALIZED;
            } else {
                function_pool->state = PoolState::UNINITIALIZED;
            }
        }
        function_pool->cv.notify_all();
    }).detach();

    for (int i = 0; i < max_pool_size; ++i) {
        std::thread([version, function_pool, i, max_pool_size, init_finished_count]() {
            SCOPED_INIT_THREAD_CONTEXT();
            ProcessPtr process;
            Status status = PythonServerManager::fork(version, &process);
            const bool ok = status.ok() && process;
            ProcessPtr process_to_shutdown;
            {
                std::scoped_lock lock(function_pool->mutex);
                // shutdown() and repair can race with detached init workers after timeout.
                // Late successful forks only fill slots that are still empty or dead.
                if (ok &&
                    (function_pool->state == PoolState::INITIALIZING ||
                     function_pool->state == PoolState::INITIALIZED) &&
                    i < function_pool->processes.size() &&
                    (!function_pool->processes[i] || !function_pool->processes[i]->is_alive())) {
                    // Register only accepted processes; rejected forks shut down separately.
                    function_pool->process_registry->add(process);
                    process->set_package_cache_entry(function_pool->package_cache_entry);
                    function_pool->processes[i] = std::move(process);
                    function_pool->has_available_process = true;
                } else if (ok) {
                    process_to_shutdown = std::move(process);
                } else [[unlikely]] {
                    LOG(WARNING) << "Failed to create Python process " << (i + 1) << "/"
                                 << max_pool_size << " for version " << version.to_string() << ": "
                                 << status.to_string();
                }
            }
            init_finished_count->fetch_add(1, std::memory_order_acq_rel);
            function_pool->cv.notify_all();
            if (process_to_shutdown) {
                process_to_shutdown->shutdown();
            }
            TEST_SYNC_POINT_CALLBACK(
                    "PythonServerManager::_ensure_pool_initialized:init_worker_finished", &version,
                    init_finished_count.get());
        }).detach();
    }
}

Result<std::shared_ptr<PythonServerManager::FunctionProcessPool>>
PythonServerManager::_ensure_pool_initialized(
        const PythonUDFMeta& meta, const PythonVersion& version,
        std::shared_ptr<UserFunctionCacheEntry> package_cache_entry,
        std::shared_ptr<PythonUDFPoolLease>* lease) {
    auto function_pool_result =
            _get_or_create_process_pool(meta, version, std::move(package_cache_entry), lease);
    if (!function_pool_result.has_value()) {
        return ResultError(function_pool_result.error());
    }
    auto function_pool = function_pool_result.value();
    auto process_pool_init_timeout = PROCESS_POOL_INIT_TIMEOUT;
    TEST_SYNC_POINT_CALLBACK(
            "PythonServerManager::_ensure_pool_initialized:process_pool_init_timeout",
            &process_pool_init_timeout);

    std::unique_lock<std::mutex> lock(function_pool->mutex);
    if (function_pool->state == PoolState::INITIALIZED) {
        return function_pool;
    }

    if (function_pool->state != PoolState::STOPPED) {
        if (function_pool->state != PoolState::INITIALIZING) {
            _start_pool_initialization(function_pool, version, process_pool_init_timeout);
        }

        // Wait only for the first usable process. INITIALIZED is set later by the last init worker
        // after every slot has attempted initialization.
        function_pool->cv.wait_for(lock, process_pool_init_timeout, [&function_pool]() {
            return function_pool->has_available_process ||
                   function_pool->state == PoolState::STOPPED ||
                   function_pool->state != PoolState::INITIALIZING;
        });
        if (function_pool->has_available_process) {
            return function_pool;
        }
        function_pool->cv.notify_all();
    }

    return ResultError(Status::Error<ErrorCode::SERVICE_UNAVAILABLE>(
            "Failed to initialize Python process pool for version {}: no process became available "
            "within {} ms",
            version.to_string(), process_pool_init_timeout.count()));
}

void PythonServerManager::_start_pool_repair(
        const PythonVersion& version, const std::shared_ptr<FunctionProcessPool>& function_pool) {
    // Called with the pool lock held so concurrent requests cannot schedule the same repair.
    function_pool->repairing = true;
    // Repair is done in the background because fork can be slow. The current request still
    // waits briefly below so a transient all-dead pool can recover without failing.
    std::thread([version, function_pool]() {
        SCOPED_INIT_THREAD_CONTEXT();
        int recreated = PythonServerManager::_repair_process_pool(version, function_pool);
        {
            std::scoped_lock lock(function_pool->mutex);
            function_pool->repairing = false;
        }
        function_pool->cv.notify_all();
        if (recreated > 0) {
            LOG(INFO) << "Repaired Python process pool for version " << version.to_string()
                      << ": recreated=" << recreated;
        }
    }).detach();
}

Status PythonServerManager::_get_process(const PythonVersion& version,
                                         const std::shared_ptr<FunctionProcessPool>& function_pool,
                                         ProcessPtr* process) {
    {
        std::unique_lock<std::mutex> lock(function_pool->mutex);
        std::vector<ProcessPtr>& pool = function_pool->processes;

        if (function_pool->state == PoolState::STOPPED) {
            function_pool->has_available_process = false;
            return Status::Error<ErrorCode::SERVICE_UNAVAILABLE>(
                    "Python process pool has stopped for version {}", version.to_string());
        }

        if (_select_alive_process_from_pool(pool, process)) [[likely]] {
            function_pool->has_available_process = true;
            return Status::OK();
        }
        function_pool->has_available_process = false;

        if (function_pool->state == PoolState::INITIALIZING) {
            function_pool->cv.wait_for(lock, PROCESS_REPAIR_WAIT_TIMEOUT, [&function_pool]() {
                return std::any_of(function_pool->processes.begin(), function_pool->processes.end(),
                                   [](const ProcessPtr& p) { return p && p->is_alive(); }) ||
                       function_pool->state != PoolState::INITIALIZING;
            });
            if (function_pool->state == PoolState::STOPPED) {
                return Status::Error<ErrorCode::SERVICE_UNAVAILABLE>(
                        "Python process pool has stopped for version {}", version.to_string());
            }
            if (_select_alive_process_from_pool(pool, process)) {
                function_pool->has_available_process = true;
                return Status::OK();
            }
            function_pool->has_available_process = false;
            return Status::Error<ErrorCode::SERVICE_UNAVAILABLE>(
                    "Python process pool is initializing but has no available process for version "
                    "{} after waiting {} ms",
                    version.to_string(), PROCESS_REPAIR_WAIT_TIMEOUT.count());
        }

        if (function_pool->state != PoolState::INITIALIZED) {
            return Status::Error<ErrorCode::SERVICE_UNAVAILABLE>(
                    "Python process pool is not initialized for version {}", version.to_string());
        }

        if (!function_pool->repairing) {
            _start_pool_repair(version, function_pool);
        }

        // Keep the request recoverable in the common case where the Python runtime can fork
        // normally and only the existing pool entries died. The wait is short so a wedged fork path
        // still returns SERVICE_UNAVAILABLE promptly.
        function_pool->cv.wait_for(lock, PROCESS_REPAIR_WAIT_TIMEOUT, [&function_pool]() {
            return std::any_of(function_pool->processes.begin(), function_pool->processes.end(),
                               [](const ProcessPtr& p) { return p && p->is_alive(); }) ||
                   function_pool->state == PoolState::STOPPED;
        });
        if (function_pool->state == PoolState::STOPPED) {
            function_pool->has_available_process = false;
            return Status::Error<ErrorCode::SERVICE_UNAVAILABLE>(
                    "Python process pool has stopped for version {}", version.to_string());
        }

        if (_select_alive_process_from_pool(pool, process)) {
            function_pool->has_available_process = true;
            return Status::OK();
        }
        function_pool->has_available_process = false;
    }

    return Status::Error<ErrorCode::SERVICE_UNAVAILABLE>(
            "Python process pool has no available process for version {} after waiting repair for "
            "{} ms",
            version.to_string(), PROCESS_REPAIR_WAIT_TIMEOUT.count());
}

Status PythonServerManager::fork(const PythonVersion& version, ProcessPtr* process) {
    std::string python_executable_path = version.get_executable_path();
    std::string fight_server_path = get_fight_server_path();
    std::string base_unix_socket_path = get_base_unix_socket_path();
    std::vector<std::string> args = {"-u", fight_server_path, base_unix_socket_path};
    boost::process::environment env = boost::this_process::environment();
    boost::process::ipstream child_output;

    // Test synchronization belongs before child creation so it cannot consume readiness timeout.
    TEST_SYNC_POINT_CALLBACK("PythonServerManager::fork:before_process_start", &version);

    try {
        boost::process::child c(
                python_executable_path, args, boost::process::std_out > child_output,
                boost::process::env = env,
                boost::process::on_exit([](int exit_code, const std::error_code& ec) {
                    if (ec) {
                        LOG(WARNING) << "Python UDF server exited with error: " << ec.message();
                    }
                }));

        // Bound socket readiness: a child process can start but never create the Flight socket.
        // Without this, pool initialization can block until FE reports send-fragments RPC timeout.
        pid_t child_pid = c.id();
        std::string expected_socket_path = get_unix_socket_file_path(child_pid);
        bool started_successfully = false;
        std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();

        while (std::chrono::steady_clock::now() - start < PROCESS_START_TIMEOUT) {
            struct stat buffer;
            if (stat(expected_socket_path.c_str(), &buffer) == 0) {
                started_successfully = true;
                break;
            }

            if (!c.running()) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        if (!started_successfully) {
            int exit_status = 0;
            if (c.running()) {
                // Don't use the `wait` of boost::process, but use the operating system signal and waitpid with timeout instead.
                // Because boost::process may block the initialization/repair thread for a long time,
                // exceeding the timeout limit expected by the process pool.
                ::kill(child_pid, SIGTERM);
                auto wait_result = PythonUDFProcess::wait_child_exit(
                        child_pid, PROCESS_TERMINATE_TIMEOUT, &exit_status);
                if (wait_result == PythonUDFProcess::ChildExitWaitResult::TIMEOUT ||
                    wait_result == PythonUDFProcess::ChildExitWaitResult::ERROR) {
                    LOG(WARNING) << "Python server start timeout and terminate timeout exceeded,"
                                 << " sending SIGKILL to pid=" << child_pid;
                    ::kill(child_pid, SIGKILL);
                    wait_result = PythonUDFProcess::wait_child_exit(
                            child_pid, PROCESS_TERMINATE_TIMEOUT, &exit_status);
                    if (wait_result == PythonUDFProcess::ChildExitWaitResult::TIMEOUT ||
                        wait_result == PythonUDFProcess::ChildExitWaitResult::ERROR) [[unlikely]] {
                        // The child was SIGKILLed but not reaped within the bounded wait. Do not
                        // drop waitpid ownership after detach; otherwise a later exit can leave a
                        // zombie Python process under BE.
                        PythonUDFProcess::enqueue_child_for_reap(child_pid);
                        c.detach();
                        return Status::InternalError(
                                "Python server start failed: process did not exit after SIGKILL, "
                                "pid={}",
                                child_pid);
                    }
                }
            } else {
                PythonUDFProcess::wait_child_exit(child_pid, std::chrono::milliseconds(0),
                                                  &exit_status);
            }
            c.detach();
            return Status::InternalError("Python server start failed: socket file not found at {}",
                                         expected_socket_path);
        }

        *process = std::make_shared<PythonUDFProcess>(std::move(c), std::move(child_output));

    } catch (const std::exception& e) {
        return Status::InternalError("Failed to start Python UDF server: {}", e.what());
    }

    return Status::OK();
}

void PythonServerManager::_reclaim_idle_pools(std::chrono::steady_clock::time_point now) {
    std::vector<std::shared_ptr<FunctionProcessPool>> retired;
    {
        std::scoped_lock lock(_pools_mutex);
        std::erase_if(_process_pools, [&](const auto& entry) {
            const auto& pool = entry.second;
            std::scoped_lock pool_lock(pool->mutex);
            // 1. Never reclaim a pool while any client still holds it.
            // 2. A STOPPED pool is reclaimed on the next scan after its last client closes,
            //    regardless of auto_suspend.
            // 3. Other pools require auto_suspend >= 0 and no clients for that many seconds.
            //    The last client closing resets idle_since, so each idle period starts afresh.
            if (pool->active_clients != 0 ||
                (pool->state != PoolState::STOPPED &&
                 (pool->auto_suspend < 0 ||
                  std::chrono::duration_cast<std::chrono::seconds>(now - pool->idle_since).count() <
                          pool->auto_suspend))) {
                return false;
            }
            // Prevent detached initialization/repair workers from publishing into a retired pool.
            pool->state = PoolState::STOPPED;
            pool->has_available_process = false;
            retired.emplace_back(pool);
            return true;
        });
    }
    for (const auto& pool : retired) {
        pool->cv.notify_all();
        std::vector<ProcessPtr> processes;
        {
            std::scoped_lock lock(pool->mutex);
            processes.swap(pool->processes);
        }
        for (const auto& process : processes) {
            if (process) {
                process->shutdown();
            }
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

            if (_shutdown_flag.load(std::memory_order_acquire)) break;

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
        {
            std::scoped_lock lock(function_pool->mutex);
            if (function_pool->state != PoolState::INITIALIZED || function_pool->repairing) {
                continue;
            }
            // Share the same repair guard with foreground requests. Otherwise health check and
            // _get_process() can fork the same empty/dead slots at the same time under failures.
            function_pool->repairing = true;
        }
        int recreated = _repair_process_pool(function_pool->version, function_pool);
        {
            std::scoped_lock lock(function_pool->mutex);
            function_pool->repairing = false;
        }
        function_pool->cv.notify_all();
        total_recreated += recreated;
    }

    if (total_recreated > 0) {
        LOG(INFO) << "Health check completed: recreated=" << total_recreated;
    }
}

int PythonServerManager::_repair_process_pool(
        const PythonVersion& version, const std::shared_ptr<FunctionProcessPool>& function_pool) {
    const int max_pool_size = function_pool->pool_size;
    std::vector<size_t> died_process_indices;
    {
        std::scoped_lock lock(function_pool->mutex);
        if (function_pool->state != PoolState::INITIALIZED) {
            return 0;
        }

        auto& pool = function_pool->processes;
        died_process_indices.reserve(std::max<size_t>(pool.size(), max_pool_size));
        // Need to fix the following two cases
        // 1. Existing processes have died
        // 2. Process pool not filled due to fork timeouts and other issues
        for (size_t i = 0; i < pool.size(); ++i) {
            const auto& process = pool[i];
            if (!process || !process->is_alive()) {
                died_process_indices.push_back(i);
            }
        }
        for (size_t i = pool.size(); i < static_cast<size_t>(max_pool_size); ++i) {
            died_process_indices.push_back(i);
        }
    }

    if (died_process_indices.empty()) [[likely]] {
        return 0;
    }

    int recreated = 0;
    std::vector<ProcessPtr> processes_to_shutdown;
    for (size_t index : died_process_indices) {
        {
            std::scoped_lock lock(function_pool->mutex);
            if (function_pool->state != PoolState::INITIALIZED) {
                break;
            }
        }

        ProcessPtr new_process;
        Status status = fork(version, &new_process);
        if (status.ok() && new_process) {
            bool published = false;
            {
                std::scoped_lock lock(function_pool->mutex);
                auto& pool = function_pool->processes;
                if (function_pool->state != PoolState::INITIALIZED) {
                    processes_to_shutdown.emplace_back(std::move(new_process));
                } else if (index < pool.size()) {
                    if (!pool[index] || !pool[index]->is_alive()) [[likely]] {
                        function_pool->process_registry->add(new_process);
                        new_process->set_package_cache_entry(function_pool->package_cache_entry);
                        pool[index] = std::move(new_process);
                        function_pool->has_available_process = true;
                        recreated++;
                        published = true;
                    } else {
                        processes_to_shutdown.emplace_back(std::move(new_process));
                    }
                } else if (pool.size() < static_cast<size_t>(max_pool_size)) {
                    function_pool->process_registry->add(new_process);
                    new_process->set_package_cache_entry(function_pool->package_cache_entry);
                    pool.emplace_back(std::move(new_process));
                    function_pool->has_available_process = true;
                    recreated++;
                    published = true;
                } else {
                    processes_to_shutdown.emplace_back(std::move(new_process));
                }
            }
            if (published) {
                function_pool->cv.notify_all();
            }
        } else {
            LOG(ERROR) << "Failed to recreate Python process for version " << version.to_string()
                       << ": " << status.to_string();
        }
    }

    {
        std::scoped_lock lock(function_pool->mutex);
        auto& pool = function_pool->processes;

        if (function_pool->state != PoolState::INITIALIZED) {
            function_pool->has_available_process = false;
        } else {
            // Keep empty/dead slots instead of shrinking the vector. Init workers are detached and
            // publish by original slot index; shrinking here would make a late successful init look
            // out-of-range and discard a usable process.
            function_pool->has_available_process = std::ranges::any_of(
                    pool, [](const ProcessPtr& process) { return process && process->is_alive(); });
        }
    }
    function_pool->cv.notify_all();
    for (auto& process : processes_to_shutdown) {
        process->shutdown();
    }
    return recreated;
}

void PythonServerManager::shutdown() {
    std::vector<std::shared_ptr<FunctionProcessPool>> pools_to_shutdown;
    {
        std::scoped_lock lock(_pools_mutex);
        _shutdown_flag.store(true, std::memory_order_release);
        pools_to_shutdown.reserve(_process_pools.size());
        for (auto& [function_id, function_pool] : _process_pools) {
            pools_to_shutdown.emplace_back(std::move(function_pool));
        }
        _process_pools.clear();
    }

    for (const auto& function_pool : pools_to_shutdown) {
        if (!function_pool) {
            continue;
        }
        std::scoped_lock lock(function_pool->mutex);
        function_pool->state = PoolState::STOPPED;
        function_pool->has_available_process = false;
        function_pool->repairing = false;
        function_pool->cv.notify_all();
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

    // Include processes retained by clients after DROP. Snapshot ownership keeps pool clearing
    // from destroying a process under the pool lock; all shutdowns happen outside locks.
    auto processes_to_shutdown = _process_registry->snapshot();
    for (const auto& function_pool : pools_to_shutdown) {
        if (!function_pool) {
            continue;
        }
        std::scoped_lock lock(function_pool->mutex);
        function_pool->processes.clear();
        function_pool->cv.notify_all();
    }
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
    std::shared_ptr<FunctionProcessPool> pool;
    {
        std::scoped_lock map_lock(_pools_mutex);
        auto iter = _process_pools.find(function_id);
        if (iter == _process_pools.end()) {
            return;
        }
        pool = iter->second;
        // Keep the STOPPED pool in the map until the health check sees no active clients.
        // For example, a GROUP BY UDAF may process group A, receive DROP, then encounter group B.
        // B's create() must find this STOPPED pool and fail instead of starting a new pool while
        // A's client is still alive. Existing clients may finish on their current processes.
        {
            std::scoped_lock pool_lock(pool->mutex);
            pool->state = PoolState::STOPPED;
            pool->has_available_process = false;
        }
    }
    pool->cv.notify_all();
}

// Explicit template instantiation for UDF, UDAF and UDTF clients
template Status PythonServerManager::get_client<PythonUDFClient>(
        const PythonUDFMeta& func_meta, const PythonVersion& version,
        std::shared_ptr<PythonUDFClient>* client, const std::shared_ptr<arrow::Schema>& data_schema,
        std::shared_ptr<UserFunctionCacheEntry> package_cache_entry);

template Status PythonServerManager::get_client<PythonUDAFClient>(
        const PythonUDFMeta& func_meta, const PythonVersion& version,
        std::shared_ptr<PythonUDAFClient>* client,
        const std::shared_ptr<arrow::Schema>& data_schema,
        std::shared_ptr<UserFunctionCacheEntry> package_cache_entry);

template Status PythonServerManager::get_client<PythonUDTFClient>(
        const PythonUDFMeta& func_meta, const PythonVersion& version,
        std::shared_ptr<PythonUDTFClient>* client,
        const std::shared_ptr<arrow::Schema>& data_schema,
        std::shared_ptr<UserFunctionCacheEntry> package_cache_entry);

} // namespace doris
