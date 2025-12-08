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
#include <sys/wait.h>
#include <unistd.h>

#include <boost/process.hpp>

#include "common/logging.h"

namespace doris {

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

    _child.terminate();
    bool graceful = false;
    constexpr std::chrono::milliseconds retry_interval(100); // 100ms

    for (int i = 0; i < TERMINATE_RETRY_TIMES; ++i) {
        if (!_child.running()) {
            graceful = true;
            break;
        }
        std::this_thread::sleep_for(retry_interval);
    }

    if (!graceful) {
        LOG(WARNING) << "Python process did not terminate gracefully, sending SIGKILL";
        ::kill(_child_pid, SIGKILL);
        _child.wait();
    }

    if (int exit_code = _child.exit_code(); exit_code > 128 && exit_code <= 255) {
        int signal = exit_code - 128;
        LOG(INFO) << "Python process was killed by signal " << signal;
    } else {
        LOG(INFO) << "Python process exited normally with code: " << exit_code;
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