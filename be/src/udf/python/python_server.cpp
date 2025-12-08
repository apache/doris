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

namespace doris {

thread_local std::unordered_map<PythonVersion, ProcessPtr> PythonServerManager::_processes;
std::vector<ShutdownCallback> PythonServerManager::_shutdown_callbacks;
std::mutex PythonServerManager::_callbacks_mutex;

template <typename T>
Status PythonServerManager::get_client(const PythonUDFMeta& func_meta, const PythonVersion& version,
                                       std::shared_ptr<T>* client) {
    // Ensure thread-local shutdown callback is registered (called once per thread)
    static thread_local std::once_flag register_flag;
    std::call_once(register_flag, []() {
        instance().register_thread_shutdown([&processes = _processes]() {
            if (processes.empty()) return;
            for (auto& [version, process] : processes) {
                if (process) {
                    process->shutdown();
                    LOG(INFO) << "Shutdown Python process for version " << version.to_string()
                              << " in thread " << std::this_thread::get_id();
                }
            }
            processes.clear();
        });
    });

    ProcessPtr process;
    RETURN_IF_ERROR(get_process(version, &process));
    RETURN_IF_ERROR(T::create(func_meta, std::move(process), client));
    return Status::OK();
}

Status PythonServerManager::get_process(const PythonVersion& version, ProcessPtr* process) {
    // Check if process already exists for this thread and version
    auto it = _processes.find(version);
    if (it != _processes.end() && it->second && it->second->is_alive()) {
        // Process exists and is alive, return shared ownership
        *process = it->second;
        return Status::OK();
    }

    ProcessPtr new_process;
    RETURN_IF_ERROR(fork(version, &new_process));
    _processes[version] = new_process;
    *process = new_process;

    LOG(INFO) << "Created thread-local Python process for version " << version.to_string()
              << ", thread_id: " << std::this_thread::get_id();

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

void PythonServerManager::register_thread_shutdown(ShutdownCallback&& callback) {
    std::lock_guard<std::mutex> lock(_callbacks_mutex);
    _shutdown_callbacks.push_back(std::move(callback));
}

void PythonServerManager::shutdown() {
    LOG(INFO) << "Shutting down all Python UDF processes across " << _shutdown_callbacks.size()
              << " threads";
    {
        std::lock_guard<std::mutex> lock(_callbacks_mutex);
        // Execute all registered shutdown callbacks
        for (auto& callback : _shutdown_callbacks) {
            callback();
        }
        _shutdown_callbacks.clear();
    }
    LOG(INFO) << "All Python UDF processes shut down successfully";
}

// Explicit template instantiation for UDF, UDAF and UDTF clients
template Status PythonServerManager::get_client<PythonUDFClient>(
        const PythonUDFMeta& func_meta, const PythonVersion& version,
        std::shared_ptr<PythonUDFClient>* client);

template Status PythonServerManager::get_client<PythonUDAFClient>(
        const PythonUDFMeta& func_meta, const PythonVersion& version,
        std::shared_ptr<PythonUDAFClient>* client);

template Status PythonServerManager::get_client<PythonUDTFClient>(
        const PythonUDFMeta& func_meta, const PythonVersion& version,
        std::shared_ptr<PythonUDTFClient>* client);

} // namespace doris