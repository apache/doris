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

#include "udf/python/python_udf_runtime.h"

#include <butil/fd_utility.h>
#include <signal.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#include <boost/process.hpp>
#include <cerrno>
#include <chrono>
#include <thread>

#include "common/logging.h"

namespace doris {

PythonUDFProcess::ChildExitWaitResult PythonUDFProcess::wait_child_exit(
        pid_t pid, std::chrono::milliseconds timeout, int* exit_status) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (true) {
        pid_t ret = waitpid(pid, exit_status, WNOHANG);
        if (ret == pid) {
            return ChildExitWaitResult::EXITED;
        }
        if (ret < 0) {
            if (errno == EINTR) {
                // retry if interrupted
                continue;
            }
            // Another owner may already have observed the child exit through boost::process.
            if (errno == ECHILD) {
                return ChildExitWaitResult::ALREADY_REAPED;
            }
            LOG(WARNING) << "Failed to wait Python process pid=" << pid << ": " << strerror(errno);
            return ChildExitWaitResult::ERROR;
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            return ChildExitWaitResult::TIMEOUT;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

void PythonUDFProcess::remove_unix_socket() {
    if (_uri.empty() || _unix_socket_file_path.empty()) return;

    if (unlink(_unix_socket_file_path.c_str()) == 0) {
        LOG(INFO) << "Successfully removed unix socket: " << _unix_socket_file_path;
        return;
    }

    if (errno == ENOENT) {
        // File does not exist, this is fine, no need to warn
        LOG(INFO) << "Unix socket not found (already removed): " << _uri;
    } else {
        LOG(WARNING) << "Failed to remove unix socket " << _uri << ": " << std::strerror(errno)
                     << " (errno=" << errno << ")";
    }
}

void PythonUDFProcess::shutdown() {
    if (!_child.valid() || _is_shutdown) return;

    constexpr std::chrono::milliseconds terminate_timeout(1000);
    int exit_status = 0;
    bool exited = !_child.running();
    bool status_available = false;
    bool already_reaped = false;
    if (!exited) {
        ::kill(_child_pid, SIGTERM);
        auto wait_result = wait_child_exit(_child_pid, terminate_timeout, &exit_status);
        exited = wait_result == ChildExitWaitResult::EXITED ||
                 wait_result == ChildExitWaitResult::ALREADY_REAPED;
        status_available = wait_result == ChildExitWaitResult::EXITED;
        already_reaped = wait_result == ChildExitWaitResult::ALREADY_REAPED;
    } else {
        auto wait_result = wait_child_exit(_child_pid, std::chrono::milliseconds(0), &exit_status);
        status_available = wait_result == ChildExitWaitResult::EXITED;
        already_reaped = wait_result == ChildExitWaitResult::ALREADY_REAPED;
    }

    if (!exited) {
        LOG(WARNING) << "Python process did not terminate gracefully, sending SIGKILL, pid="
                     << _child_pid;
        ::kill(_child_pid, SIGKILL);
        auto wait_result = wait_child_exit(_child_pid, terminate_timeout, &exit_status);
        exited = wait_result == ChildExitWaitResult::EXITED ||
                 wait_result == ChildExitWaitResult::ALREADY_REAPED;
        status_available = wait_result == ChildExitWaitResult::EXITED;
        already_reaped = wait_result == ChildExitWaitResult::ALREADY_REAPED;
    }
    _child.detach();

    if (!exited) [[unlikely]] {
        LOG(WARNING) << "Python process did not exit after SIGKILL, detach child handle, pid="
                     << _child_pid;
    } else if (already_reaped) {
        LOG(INFO) << "Python process already reaped by another owner, pid=" << _child_pid;
    } else if (!status_available) {
        LOG(INFO) << "Python process exited but exit status is unavailable, pid=" << _child_pid;
    } else {
        if (WIFSIGNALED(exit_status)) {
            LOG(INFO) << "Python process was killed by signal " << WTERMSIG(exit_status);
        } else if (WIFEXITED(exit_status)) {
            LOG(INFO) << "Python process exited normally with code: " << WEXITSTATUS(exit_status);
        } else {
            LOG(INFO) << "Python process exited";
        }
    }

    _output_stream.close();
    remove_unix_socket();
    _is_shutdown = true;
}

std::string PythonUDFProcess::to_string() const {
    return fmt::format(
            "PythonUDFProcess(child_pid={}, uri={}, "
            "unix_socket_file_path={}, is_shutdown={})",
            _child_pid, _uri, _unix_socket_file_path, _is_shutdown);
}

} // namespace doris
