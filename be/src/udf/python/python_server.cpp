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
#include "runtime/thread_context.h"
#include "udf/python/python_udaf_client.h"
#include "udf/python/python_udf_client.h"
#include "udf/python/python_udtf_client.h"
#include "util/cpu_info.h"

namespace doris {

Result<std::shared_ptr<PythonServerManager::VersionedProcessPool>>
PythonServerManager::_get_or_create_process_pool(const PythonVersion& version) {
    std::lock_guard<std::mutex> lock(_pools_mutex);
    // shutdown() owns the manager lifecycle. Once it starts, creating a new pool would let detached
    // init workers publish Python processes that the manager no longer tracks.
    if (_shutdown_flag.load(std::memory_order_acquire)) {
        return ResultError(Status::Error<ErrorCode::SERVICE_UNAVAILABLE>(
                "Python server manager is shutting down"));
    }
    auto& pool = _process_pools[version];
    if (!pool) {
        pool = std::make_shared<VersionedProcessPool>();
    }
    return pool;
}

std::vector<std::pair<PythonVersion, std::shared_ptr<PythonServerManager::VersionedProcessPool>>>
PythonServerManager::_snapshot_process_pools() {
    std::lock_guard<std::mutex> lock(_pools_mutex);
    std::vector<std::pair<PythonVersion, std::shared_ptr<VersionedProcessPool>>> snapshot;
    snapshot.reserve(_process_pools.size());
    for (const auto& [version, pool] : _process_pools) {
        snapshot.emplace_back(version, pool);
    }
    return snapshot;
}

#ifdef BE_TEST
void PythonServerManager::set_process_pool_for_test(const PythonVersion& version,
                                                    std::vector<ProcessPtr> processes,
                                                    bool initialized) {
    auto versioned_pool = _get_or_create_process_pool(version).value();
    std::lock_guard<std::mutex> lock(versioned_pool->mutex);
    versioned_pool->processes = std::move(processes);
    versioned_pool->initialized = initialized;
    versioned_pool->has_available_process =
            std::any_of(versioned_pool->processes.begin(), versioned_pool->processes.end(),
                        [](const ProcessPtr& process) { return process && process->is_alive(); });
    versioned_pool->stopped = false;
}

std::vector<ProcessPtr> PythonServerManager::process_pool_snapshot_for_test(
        const PythonVersion& version) {
    auto versioned_pool = _get_or_create_process_pool(version).value();
    std::lock_guard<std::mutex> lock(versioned_pool->mutex);
    return versioned_pool->processes;
}
#endif

template <typename ClientType>
Status PythonServerManager::get_client(const PythonUDFMeta& func_meta, const PythonVersion& version,
                                       std::shared_ptr<ClientType>* client,
                                       const std::shared_ptr<arrow::Schema>& data_schema) {
    std::shared_ptr<VersionedProcessPool> versioned_pool =
            DORIS_TRY(_ensure_pool_initialized(version));

    ProcessPtr process;
    RETURN_IF_ERROR(_get_process(version, versioned_pool, &process));

    if constexpr (std::is_same_v<ClientType, PythonUDAFClient>) {
        RETURN_IF_ERROR(ClientType::create(func_meta, std::move(process), data_schema, client));
    } else {
        RETURN_IF_ERROR(ClientType::create(func_meta, std::move(process), client));
    }

    return Status::OK();
}

Result<std::shared_ptr<PythonServerManager::VersionedProcessPool>>
PythonServerManager::_ensure_pool_initialized(const PythonVersion& version) {
    auto versioned_pool_result = _get_or_create_process_pool(version);
    if (!versioned_pool_result.has_value()) {
        return ResultError(versioned_pool_result.error());
    }
    auto versioned_pool = versioned_pool_result.value();
    const int max_pool_size = config::max_python_process_num > 0 ? config::max_python_process_num
                                                                 : CpuInfo::num_cores();

    bool start_health_check = false;
    {
        std::unique_lock<std::mutex> lock(versioned_pool->mutex);
        if (versioned_pool->initialized) {
            if (versioned_pool->has_available_process) {
                lock.unlock();
                _start_health_check_thread();
            }
            return versioned_pool;
        } else {
            if (versioned_pool->processes.empty()) {
                versioned_pool->has_available_process = false;
                versioned_pool->stopped = false;
                versioned_pool->processes.resize(max_pool_size);

                LOG(INFO) << "Initializing Python process pool for version " << version.to_string()
                          << " with " << max_pool_size
                          << " processes (config::max_python_process_num="
                          << config::max_python_process_num
                          << ", CPU cores=" << CpuInfo::num_cores() << ")";

                for (int i = 0; i < max_pool_size; ++i) {
                    std::thread([version, versioned_pool, i, max_pool_size]() {
                        SCOPED_INIT_THREAD_CONTEXT();
                        ProcessPtr process;
                        Status status = PythonServerManager::fork(version, &process);
                        const bool ok = status.ok() && process;
                        ProcessPtr process_to_shutdown;
                        {
                            std::lock_guard<std::mutex> lock(versioned_pool->mutex);
                            // shutdown() and repair can race with detached init workers after timeout.
                            // Late successful forks only fill slots that are still empty or dead.
                            if (ok && !versioned_pool->stopped &&
                                i < versioned_pool->processes.size() &&
                                (!versioned_pool->processes[i] ||
                                 !versioned_pool->processes[i]->is_alive())) {
                                versioned_pool->processes[i] = std::move(process);
                                versioned_pool->has_available_process = true;
                            } else if (ok) {
                                process_to_shutdown = std::move(process);
                            } else [[unlikely]] {
                                LOG(WARNING) << "Failed to create Python process " << (i + 1) << "/"
                                             << max_pool_size << " for version "
                                             << version.to_string() << ": " << status.to_string();
                            }
                        }
                        versioned_pool->cv.notify_all();
                        if (process_to_shutdown) {
                            process_to_shutdown->shutdown();
                        }
                    }).detach();
                }
            }

            // Wait only for the first usable process. The rest of the pool is best-effort and will be
            // filled by health check, so partial init failure is logged but not exposed to users.
            versioned_pool->cv.wait_for(lock, PROCESS_POOL_INIT_TIMEOUT, [&versioned_pool]() {
                return versioned_pool->has_available_process || versioned_pool->stopped;
            });
            // Mark the first init round done even when no process is available. Health check and the
            // next foreground _get_process() repair the initialized-but-empty pool without launching a
            // second full detached init round.
            versioned_pool->initialized = true;
            versioned_pool->cv.notify_all();
        }

        if (versioned_pool->has_available_process) {
            lock.unlock();
            _start_health_check_thread();
            return versioned_pool;
        }
        start_health_check = !versioned_pool->stopped;
    }

    if (start_health_check) {
        _start_health_check_thread();
    }
    return ResultError(Status::Error<ErrorCode::SERVICE_UNAVAILABLE>(
            "Failed to initialize Python process pool for version {}: no process became available "
            "within {} ms",
            version.to_string(), PROCESS_POOL_INIT_TIMEOUT.count()));
}

Status PythonServerManager::_get_process(
        const PythonVersion& version, const std::shared_ptr<VersionedProcessPool>& versioned_pool,
        ProcessPtr* process) {
    {
        std::unique_lock<std::mutex> lock(versioned_pool->mutex);
        std::vector<ProcessPtr>& pool = versioned_pool->processes;

        if (versioned_pool->stopped) {
            versioned_pool->has_available_process = false;
            return Status::Error<ErrorCode::SERVICE_UNAVAILABLE>(
                    "Python process pool has stopped for version {}", version.to_string());
        }

        auto min_alive_iter = std::min_element(pool.begin(), pool.end(),
                                               [](const ProcessPtr& a, const ProcessPtr& b) {
                                                   const bool a_alive = a && a->is_alive();
                                                   const bool b_alive = b && b->is_alive();
                                                   if (a_alive != b_alive) {
                                                       return a_alive > b_alive;
                                                   }
                                                   return a.use_count() < b.use_count();
                                               });

        if (min_alive_iter != pool.end() && *min_alive_iter && (*min_alive_iter)->is_alive())
                [[likely]] {
            versioned_pool->has_available_process = true;
            *process = *min_alive_iter;
            return Status::OK();
        }
        versioned_pool->has_available_process = false;

        if (!versioned_pool->repairing) {
            versioned_pool->repairing = true;
            // Repair is done in the background because fork can be slow. The current request still
            // waits briefly below so a transient all-dead pool can recover without failing.
            std::thread([version, versioned_pool]() {
                SCOPED_INIT_THREAD_CONTEXT();
                int recreated = PythonServerManager::_repair_process_pool(version, versioned_pool);
                {
                    std::lock_guard<std::mutex> lock(versioned_pool->mutex);
                    versioned_pool->repairing = false;
                }
                versioned_pool->cv.notify_all();
                if (recreated > 0) {
                    LOG(INFO) << "Repaired Python process pool for version " << version.to_string()
                              << ": recreated=" << recreated;
                }
            }).detach();
        }

        // Keep the request recoverable in the common case where the Python runtime can fork
        // normally and only the existing pool entries died. The wait is short so a wedged fork path
        // still returns SERVICE_UNAVAILABLE promptly.
        versioned_pool->cv.wait_for(lock, PROCESS_REPAIR_WAIT_TIMEOUT, [&versioned_pool]() {
            return std::any_of(versioned_pool->processes.begin(), versioned_pool->processes.end(),
                               [](const ProcessPtr& p) { return p && p->is_alive(); }) ||
                   versioned_pool->stopped;
        });
        if (versioned_pool->stopped) {
            versioned_pool->has_available_process = false;
            return Status::Error<ErrorCode::SERVICE_UNAVAILABLE>(
                    "Python process pool has stopped for version {}", version.to_string());
        }

        auto repaired_iter = std::min_element(pool.begin(), pool.end(),
                                              [](const ProcessPtr& a, const ProcessPtr& b) {
                                                  const bool a_alive = a && a->is_alive();
                                                  const bool b_alive = b && b->is_alive();
                                                  if (a_alive != b_alive) {
                                                      return a_alive > b_alive;
                                                  }
                                                  return a.use_count() < b.use_count();
                                              });
        if (repaired_iter != pool.end() && *repaired_iter && (*repaired_iter)->is_alive()) {
            versioned_pool->has_available_process = true;
            *process = *repaired_iter;
            return Status::OK();
        }
        versioned_pool->has_available_process = false;
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
        std::string expected_socket_path = get_unix_socket_file_path(c.id());
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
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        if (!started_successfully) {
            int exit_status = 0;
            if (c.running()) {
                // Don't use the `wait` of boost::process, but use the operating system signal and waitpid with timeout instead.
                // Because boost::process may block the initialization/repair thread for a long time,
                // exceeding the timeout limit expected by the process pool.
                ::kill(c.id(), SIGTERM);
                auto wait_result = PythonUDFProcess::wait_child_exit(
                        c.id(), PROCESS_TERMINATE_TIMEOUT, &exit_status);
                if (wait_result == PythonUDFProcess::ChildExitWaitResult::TIMEOUT ||
                    wait_result == PythonUDFProcess::ChildExitWaitResult::ERROR) {
                    LOG(WARNING) << "Python server start timeout and terminate timeout exceeded,"
                                 << " sending SIGKILL to pid=" << c.id();
                    ::kill(c.id(), SIGKILL);
                    wait_result = PythonUDFProcess::wait_child_exit(
                            c.id(), PROCESS_TERMINATE_TIMEOUT, &exit_status);
                    if (wait_result == PythonUDFProcess::ChildExitWaitResult::TIMEOUT ||
                        wait_result == PythonUDFProcess::ChildExitWaitResult::ERROR) [[unlikely]] {
                        c.detach();
                        return Status::InternalError(
                                "Python server start failed: process did not exit after SIGKILL, "
                                "pid={}",
                                c.id());
                    }
                }
            } else {
                PythonUDFProcess::wait_child_exit(c.id(), std::chrono::milliseconds(0),
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

void PythonServerManager::_start_health_check_thread() {
    std::lock_guard<std::mutex> lock(_health_check_mutex);
    if (_health_check_thread || _shutdown_flag.load(std::memory_order_acquire)) return;

    LOG(INFO) << "Starting Python process health check thread (interval: 30 seconds)";

    _health_check_thread = std::make_unique<std::thread>([this]() {
        // Health check loop
        while (!_shutdown_flag.load(std::memory_order_acquire)) {
            // Wait for interval or shutdown signal
            {
                std::unique_lock<std::mutex> lock(_health_check_mutex);
                _health_check_cv.wait_for(lock, std::chrono::seconds(30), [this]() {
                    return _shutdown_flag.load(std::memory_order_acquire);
                });
            }

            if (_shutdown_flag.load(std::memory_order_acquire)) break;

            _check_and_recreate_processes();
            _refresh_memory_stats();
        }

        LOG(INFO) << "Python process health check thread exiting";
    });
}

void PythonServerManager::_check_and_recreate_processes() {
    int total_recreated = 0;
    for (auto& [version, versioned_pool] : _snapshot_process_pools()) {
        {
            std::lock_guard<std::mutex> lock(versioned_pool->mutex);
            if (!versioned_pool->initialized || versioned_pool->stopped ||
                versioned_pool->repairing) {
                continue;
            }
            // Share the same repair guard with foreground requests. Otherwise health check and
            // _get_process() can fork the same empty/dead slots at the same time under failures.
            versioned_pool->repairing = true;
        }
        int recreated = _repair_process_pool(version, versioned_pool);
        {
            std::lock_guard<std::mutex> lock(versioned_pool->mutex);
            versioned_pool->repairing = false;
        }
        versioned_pool->cv.notify_all();
        total_recreated += recreated;
    }

    if (total_recreated > 0) {
        LOG(INFO) << "Health check completed: recreated=" << total_recreated;
    }
}

int PythonServerManager::_repair_process_pool(
        const PythonVersion& version, const std::shared_ptr<VersionedProcessPool>& versioned_pool) {
    const int max_pool_size = config::max_python_process_num > 0 ? config::max_python_process_num
                                                                 : CpuInfo::num_cores();
    std::vector<size_t> died_process_indices;
    {
        std::lock_guard<std::mutex> lock(versioned_pool->mutex);
        if (!versioned_pool->initialized || versioned_pool->stopped) {
            return 0;
        }

        auto& pool = versioned_pool->processes;
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
            std::lock_guard<std::mutex> lock(versioned_pool->mutex);
            if (!versioned_pool->initialized || versioned_pool->stopped) {
                break;
            }
        }

        ProcessPtr new_process;
        Status status = fork(version, &new_process);
        if (status.ok() && new_process) {
            bool published = false;
            {
                std::lock_guard<std::mutex> lock(versioned_pool->mutex);
                auto& pool = versioned_pool->processes;
                if (!versioned_pool->initialized || versioned_pool->stopped) {
                    processes_to_shutdown.emplace_back(std::move(new_process));
                } else if (index < pool.size()) {
                    if (!pool[index] || !pool[index]->is_alive()) [[likely]] {
                        pool[index] = std::move(new_process);
                        versioned_pool->has_available_process = true;
                        recreated++;
                        published = true;
                    } else {
                        processes_to_shutdown.emplace_back(std::move(new_process));
                    }
                } else if (pool.size() < static_cast<size_t>(max_pool_size)) {
                    pool.emplace_back(std::move(new_process));
                    versioned_pool->has_available_process = true;
                    recreated++;
                    published = true;
                } else {
                    processes_to_shutdown.emplace_back(std::move(new_process));
                }
            }
            if (published) {
                versioned_pool->cv.notify_all();
            }
        } else {
            LOG(ERROR) << "Failed to recreate Python process for version " << version.to_string()
                       << ": " << status.to_string();
        }
    }

    {
        std::lock_guard<std::mutex> lock(versioned_pool->mutex);
        auto& pool = versioned_pool->processes;

        if (!versioned_pool->initialized || versioned_pool->stopped) {
            versioned_pool->has_available_process = false;
        } else {
            // Remove slots that still failed repair. This keeps health check behavior unchanged while
            // making each successful replacement visible to foreground requests immediately.
            pool.erase(std::remove_if(pool.begin(), pool.end(),
                                      [](const ProcessPtr& process) {
                                          return !process || !process->is_alive();
                                      }),
                       pool.end());
            versioned_pool->has_available_process = std::any_of(
                    pool.begin(), pool.end(),
                    [](const ProcessPtr& process) { return process && process->is_alive(); });
        }
    }
    versioned_pool->cv.notify_all();
    for (auto& process : processes_to_shutdown) {
        process->shutdown();
    }
    return recreated;
}

void PythonServerManager::shutdown() {
    std::vector<std::pair<PythonVersion, std::shared_ptr<VersionedProcessPool>>> pools_to_shutdown;
    {
        std::lock_guard<std::mutex> lock(_pools_mutex);
        _shutdown_flag.store(true, std::memory_order_release);
        pools_to_shutdown.reserve(_process_pools.size());
        for (auto& [version, versioned_pool] : _process_pools) {
            pools_to_shutdown.emplace_back(version, std::move(versioned_pool));
        }
        _process_pools.clear();
    }

    for (auto& [version, versioned_pool] : pools_to_shutdown) {
        if (!versioned_pool) {
            continue;
        }
        std::lock_guard<std::mutex> lock(versioned_pool->mutex);
        versioned_pool->initialized = false;
        versioned_pool->has_available_process = false;
        versioned_pool->repairing = false;
        versioned_pool->stopped = true;
        versioned_pool->cv.notify_all();
    }

    std::unique_ptr<std::thread> health_check_thread;
    {
        std::lock_guard<std::mutex> lock(_health_check_mutex);
        health_check_thread = std::move(_health_check_thread);
    }
    _health_check_cv.notify_one();
    if (health_check_thread && health_check_thread->joinable()) {
        health_check_thread->join();
    }

    // Shutdown all processes
    std::vector<ProcessPtr> processes_to_shutdown;
    for (auto& [version, versioned_pool] : pools_to_shutdown) {
        if (!versioned_pool) {
            continue;
        }
        std::lock_guard<std::mutex> lock(versioned_pool->mutex);
        auto& pool = versioned_pool->processes;
        for (auto& process : pool) {
            if (process) {
                processes_to_shutdown.emplace_back(std::move(process));
            }
        }
        pool.clear();
        versioned_pool->cv.notify_all();
    }
    for (auto& process : processes_to_shutdown) {
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

    for (const auto& [version, versioned_pool] : _snapshot_process_pools()) {
        std::lock_guard<std::mutex> lock(versioned_pool->mutex);
        const auto& pool = versioned_pool->processes;
        for (const auto& process : pool) {
            if (!process || !process->is_alive()) continue;

            size_t rss_bytes = 0;
            Status s = _read_process_memory(process->get_child_pid(), &rss_bytes);

            if (s.ok()) {
                total_rss += rss_bytes;
            } else [[unlikely]] {
                LOG(WARNING) << "Failed to read memory info for Python process (pid="
                             << process->get_child_pid() << "): " << s.to_string();
            }
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

Status PythonServerManager::clear_module_cache(const std::string& location) {
    if (location.empty()) {
        return Status::InvalidArgument("Empty location for clear_module_cache");
    }

    std::string body = fmt::format(R"({{"location": "{}"}})", location);
    return _broadcast_action_to_processes("clear_module_cache", body,
                                          fmt::format("location={}", location));
}

void PythonServerManager::clear_udaf_state_cache(int64_t function_id) {
    std::string body = fmt::format(R"({{"function_id": {}}})", function_id);
    WARN_IF_ERROR(_broadcast_action_to_processes("clear_udaf_state_cache", body,
                                                 fmt::format("function_id={}", function_id)),
                  "failed to clear Python UDAF state cache");
}

Status PythonServerManager::_broadcast_action_to_processes(const std::string& action_type,
                                                           const std::string& body,
                                                           const std::string& log_name) {
    int success_count = 0;
    int fail_count = 0;
    bool has_active_process = false;

    for (auto& [version, versioned_pool] : _snapshot_process_pools()) {
        std::lock_guard<std::mutex> lock(versioned_pool->mutex);
        auto& pool = versioned_pool->processes;
        for (auto& process : pool) {
            if (!process || !process->is_alive()) {
                continue;
            }
            has_active_process = true;
            try {
                auto loc_result = arrow::flight::Location::Parse(process->get_uri());
                if (!loc_result.ok()) [[unlikely]] {
                    fail_count++;
                    continue;
                }

                auto client_result = arrow::flight::FlightClient::Connect(*loc_result);
                if (!client_result.ok()) [[unlikely]] {
                    fail_count++;
                    continue;
                }
                auto client = std::move(*client_result);

                arrow::flight::Action action;
                action.type = action_type;
                action.body = arrow::Buffer::FromString(body);

                auto result_stream = client->DoAction(action);
                if (!result_stream.ok()) {
                    fail_count++;
                    continue;
                }

                auto result = (*result_stream)->Next();
                if (result.ok() && *result) {
                    success_count++;
                } else {
                    fail_count++;
                }

            } catch (...) {
                fail_count++;
            }
        }
    }

    if (!has_active_process) {
        return Status::OK();
    }

    LOG(INFO) << action_type << " completed for " << log_name << ", success=" << success_count
              << ", failed=" << fail_count;

    if (fail_count > 0) {
        return Status::InternalError("{} failed for {}, success={}, failed={}", action_type,
                                     log_name, success_count, fail_count);
    }

    return Status::OK();
}

// Explicit template instantiation for UDF, UDAF and UDTF clients
template Status PythonServerManager::get_client<PythonUDFClient>(
        const PythonUDFMeta& func_meta, const PythonVersion& version,
        std::shared_ptr<PythonUDFClient>* client,
        const std::shared_ptr<arrow::Schema>& data_schema);

template Status PythonServerManager::get_client<PythonUDAFClient>(
        const PythonUDFMeta& func_meta, const PythonVersion& version,
        std::shared_ptr<PythonUDAFClient>* client,
        const std::shared_ptr<arrow::Schema>& data_schema);

template Status PythonServerManager::get_client<PythonUDTFClient>(
        const PythonUDFMeta& func_meta, const PythonVersion& version,
        std::shared_ptr<PythonUDTFClient>* client,
        const std::shared_ptr<arrow::Schema>& data_schema);

} // namespace doris
