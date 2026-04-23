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
#include <sys/stat.h>

#include <boost/asio.hpp>
#include <boost/process.hpp>

#include "common/config.h"
#include "udf/python/python_udaf_client.h"
#include "udf/python/python_udf_client.h"
#include "udf/python/python_udtf_client.h"
#include "util/cpu_info.h"

namespace doris {

template <typename T>
Status PythonServerManager::get_client(const PythonUDFMeta& func_meta, const PythonVersion& version,
                                       std::shared_ptr<T>* client,
                                       const std::shared_ptr<arrow::Schema>& data_schema) {
    // Ensure process pool is initialized for this version
    RETURN_IF_ERROR(ensure_pool_initialized(version));

    ProcessPtr process;
    RETURN_IF_ERROR(get_process(version, &process));

    if constexpr (std::is_same_v<T, PythonUDAFClient>) {
        RETURN_IF_ERROR(T::create(func_meta, std::move(process), data_schema, client));
    } else {
        RETURN_IF_ERROR(T::create(func_meta, std::move(process), client));
    }

    return Status::OK();
}

Status PythonServerManager::ensure_pool_initialized(const PythonVersion& version) {
    std::lock_guard<std::mutex> lock(_pools_mutex);

    // Check if already initialized
    if (_initialized_versions.count(version)) return Status::OK();

    std::vector<ProcessPtr>& pool = _process_pools[version];
    // 0 means use CPU core count as default, otherwise use the specified value
    int max_pool_size = config::max_python_process_num > 0 ? config::max_python_process_num
                                                           : CpuInfo::num_cores();

    LOG(INFO) << "Initializing Python process pool for version " << version.to_string() << " with "
              << max_pool_size
              << " processes (config::max_python_process_num=" << config::max_python_process_num
              << ", CPU cores=" << CpuInfo::num_cores() << ")";

    std::vector<std::future<Status>> futures;
    std::vector<ProcessPtr> temp_processes(max_pool_size);

    for (int i = 0; i < max_pool_size; i++) {
        futures.push_back(std::async(std::launch::async, [this, &version, i, &temp_processes]() {
            ProcessPtr process;
            Status s = fork(version, &process);
            if (s.ok()) {
                temp_processes[i] = std::move(process);
            }
            return s;
        }));
    }

    int success_count = 0;
    int failure_count = 0;
    for (int i = 0; i < max_pool_size; i++) {
        Status s = futures[i].get();
        if (s.ok() && temp_processes[i]) {
            pool.push_back(std::move(temp_processes[i]));
            success_count++;
        } else {
            failure_count++;
            LOG(WARNING) << "Failed to create Python process " << (i + 1) << "/" << max_pool_size
                         << ": " << s.to_string();
        }
    }

    if (pool.empty()) {
        return Status::InternalError(
                "Failed to initialize Python process pool: all {} process creation attempts failed",
                max_pool_size);
    }

    LOG(INFO) << "Python process pool initialized for version " << version.to_string()
              << ": created " << success_count << " processes"
              << (failure_count > 0 ? fmt::format(" ({} failed)", failure_count) : "");

    _initialized_versions.insert(version);
    _start_health_check_thread();

    return Status::OK();
}

Status PythonServerManager::get_process(const PythonVersion& version, ProcessPtr* process) {
    std::lock_guard<std::mutex> lock(_pools_mutex);
    std::vector<ProcessPtr>& pool = _process_pools[version];

    if (UNLIKELY(pool.empty())) {
        return Status::InternalError("Python process pool is empty for version {}",
                                     version.to_string());
    }

    // Find process with minimum load (use_count - 1 gives active client count)
    auto min_iter = std::min_element(
            pool.begin(), pool.end(),
            [](const ProcessPtr& a, const ProcessPtr& b) { return a.use_count() < b.use_count(); });

    // Return process with minimum load
    *process = *min_iter;
    return Status::OK();
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

        // Wait for socket file to be created (indicates server is ready)
        std::string expected_socket_path = get_unix_socket_file_path(c.id());
        bool started_successfully = false;
        std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();
        const auto timeout = std::chrono::milliseconds(5000);

        while (std::chrono::steady_clock::now() - start < timeout) {
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
            if (c.running()) {
                c.terminate();
                c.wait();
            }
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
    if (_health_check_thread) return;

    LOG(INFO) << "Starting Python process health check thread (interval: 60 seconds)";

    _health_check_thread = std::make_unique<std::thread>([this]() {
        // Health check loop
        while (!_shutdown_flag.load(std::memory_order_acquire)) {
            // Wait for interval or shutdown signal
            {
                std::unique_lock<std::mutex> lock(_health_check_mutex);
                _health_check_cv.wait_for(lock, std::chrono::seconds(60), [this]() {
                    return _shutdown_flag.load(std::memory_order_acquire);
                });
            }

            if (_shutdown_flag.load(std::memory_order_acquire)) break;

            std::lock_guard<std::mutex> lock(_pools_mutex);

            int total_checked = 0;
            int total_dead = 0;
            int total_recreated = 0;

            for (auto& [version, pool] : _process_pools) {
                for (size_t i = 0; i < pool.size(); ++i) {
                    auto& process = pool[i];
                    if (!process) continue;

                    total_checked++;
                    if (!process->is_alive()) {
                        total_dead++;
                        LOG(WARNING)
                                << "Detected dead Python process (pid=" << process->get_child_pid()
                                << ", version=" << version.to_string() << "), recreating...";

                        ProcessPtr new_process;
                        Status s = fork(version, &new_process);
                        if (s.ok()) {
                            pool[i] = std::move(new_process);
                            total_recreated++;
                            LOG(INFO) << "Successfully recreated Python process for version "
                                      << version.to_string();
                        } else {
                            LOG(ERROR) << "Failed to recreate Python process for version "
                                       << version.to_string() << ": " << s.to_string();
                            pool.erase(pool.begin() + i);
                            --i;
                        }
                    }
                }
            }

            if (total_dead > 0) {
                LOG(INFO) << "Health check completed: checked=" << total_checked
                          << ", dead=" << total_dead << ", recreated=" << total_recreated;
            }
        }

        LOG(INFO) << "Python process health check thread exiting";
    });
}

void PythonServerManager::shutdown() {
    // Signal health check thread to stop
    _shutdown_flag.store(true, std::memory_order_release);
    _health_check_cv.notify_one();

    if (_health_check_thread && _health_check_thread->joinable()) {
        _health_check_thread->join();
        _health_check_thread.reset();
    }

    // Shutdown all processes
    std::lock_guard<std::mutex> lock(_pools_mutex);
    for (auto& [version, pool] : _process_pools) {
        for (auto& process : pool) {
            if (process) {
                process->shutdown();
            }
        }
    }
    _process_pools.clear();
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