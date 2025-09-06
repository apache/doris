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

#include "udf/python/python_udf_server.h"

#include <butil/fd_utility.h>
#include <dirent.h>
#include <fmt/core.h>
#include <sys/poll.h>

#include <boost/asio.hpp>
#include <boost/process.hpp>

#include "common/config.h"
#include "udf/python/python_udf_client.h"

namespace doris {

Status PythonUDFServerManager::init(const std::vector<PythonVersion>& versions) {
    std::lock_guard<std::mutex> lock(_pools_mutex);
    for (const auto& version : versions) {
        if (_pools.find(version) != _pools.end()) continue;
        PythonUDFProcessPoolPtr new_pool = std::make_unique<PythonUDFProcessPool>(
                version, config::max_python_process_nums, config::min_python_process_nums);
        RETURN_IF_ERROR(new_pool->init());
        _pools[version] = std::move(new_pool);
    }
    return Status::OK();
}

Status PythonUDFServerManager::get_client(const PythonUDFMeta& func_meta,
                                          const PythonVersion& version,
                                          PythonUDFClientPtr* client) {
    PythonUDFProcessPoolPtr* pool = nullptr;
    {
        std::lock_guard<std::mutex> lock(_pools_mutex);
        if (_pools.find(version) == _pools.end()) {
            PythonUDFProcessPoolPtr new_pool = std::make_unique<PythonUDFProcessPool>(
                    version, config::max_python_process_nums, config::min_python_process_nums);
            RETURN_IF_ERROR(new_pool->init());
            _pools[version] = std::move(new_pool);
        }
        pool = &_pools[version];
    }
    ProcessPtr process;
    RETURN_IF_ERROR((*pool)->borrow_process(&process));
    RETURN_IF_ERROR(PythonUDFClient::create(func_meta, std::move(process), client));
    return Status::OK();
}

Status PythonUDFServerManager::fork(PythonUDFProcessPool* pool, ProcessPtr* process) {
    DCHECK(pool != nullptr);
    const PythonVersion& version = pool->get_python_version();
    // e.g. /usr/local/python3.7/bin/python3
    std::string python_executable_path = version.get_executable_path();
    // e.g. /{DORIS_HOME}/plugins/python_udf/python_udf_server.py
    std::string fight_server_path = get_fight_server_path();
    // e.g. grpc+unix:///home/doris/output/be/lib/udf/python/python_udf
    std::string base_unix_socket_path = get_base_unix_socket_path();
    std::vector<std::string> args = {"-u", // unbuffered output
                                     fight_server_path, base_unix_socket_path};
    boost::process::environment env = boost::this_process::environment();
    boost::process::ipstream child_output; // input stream from child

    try {
        boost::process::child c(
                python_executable_path, args, boost::process::std_out > child_output,
                boost::process::env = env,
                boost::process::on_exit([](int exit_code, const std::error_code& ec) {
                    if (ec) {
                        LOG(WARNING) << "Python UDF server exited with error: " << ec.message();
                    }
                }));

        std::string log_line;
        std::string full_log;
        bool started_successfully = false;
        std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();
        const auto timeout = std::chrono::milliseconds(5000);

        while (std::chrono::steady_clock::now() - start < timeout) {
            if (std::getline(child_output, log_line)) {
                full_log += log_line + "\n";
                LOG(INFO) << fmt::format("Start python server, log_line: {}, full_log: {}",
                                         log_line, full_log);
                if (log_line == "Start python server successfully") {
                    started_successfully = true;
                    break;
                }
            } else {
                if (!c.running()) {
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        }

        if (!started_successfully) {
            if (c.running()) {
                c.terminate(); // terminate() sends SIGTERM on Unix
                c.wait();      // wait for exit to avoid zombie processes
            }

            std::string error_msg = full_log.empty() ? "No output from Python server" : full_log;
            LOG(ERROR) << "Python server start failed:\n" << error_msg;
            return Status::InternalError("python server start failed:\n{}", error_msg);
        }

        *process = std::make_unique<PythonUDFProcess>(std::move(c), std::move(child_output), pool);
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to start Python UDF server: {}", e.what());
    }

    return Status::OK();
}

void PythonUDFServerManager::shutdown() {
    std::lock_guard lock(_pools_mutex);
    for (auto& pool : _pools) {
        pool.second->shutdown();
    }
    _pools.clear();
    LOG(INFO) << "Python UDF server manager shutdown successfully";
}

} // namespace doris