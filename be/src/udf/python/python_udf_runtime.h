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

#include <boost/process.hpp>

#include "python_env.h"

namespace doris {

static const char* UNIX_SOCKET_PREFIX = "grpc+unix://";
// Use /tmp directory for Unix socket to avoid path length limit (max 107 chars)
static const char* BASE_UNIX_SOCKET_PATH_TEMPLATE = "/tmp/doris_python_udf";
static const char* UNIX_SOCKET_PATH_TEMPLATE = "{}_{}.sock";
static const char* FLIGHT_SERVER_PATH_TEMPLATE = "{}/plugins/python_udf/{}";
static const char* FLIGHT_SERVER_FILENAME = "python_server.py";

inline std::string get_base_unix_socket_path() {
    return fmt::format("{}{}", UNIX_SOCKET_PREFIX, BASE_UNIX_SOCKET_PATH_TEMPLATE);
}

inline std::string get_unix_socket_path(pid_t child_pid) {
    return fmt::format(UNIX_SOCKET_PATH_TEMPLATE, get_base_unix_socket_path(), child_pid);
}

inline std::string get_unix_socket_file_path(pid_t child_pid) {
    return fmt::format(UNIX_SOCKET_PATH_TEMPLATE, BASE_UNIX_SOCKET_PATH_TEMPLATE, child_pid);
}

inline std::string get_fight_server_path() {
    return fmt::format(FLIGHT_SERVER_PATH_TEMPLATE, std::getenv("DORIS_HOME"),
                       FLIGHT_SERVER_FILENAME);
}

class PythonUDFProcess;

using ProcessPtr = std::shared_ptr<PythonUDFProcess>;

class PythonUDFProcess {
public:
    PythonUDFProcess(boost::process::child child, boost::process::ipstream output_stream)
            : _is_shutdown(false),
              _child_pid(child.id()),
              _uri(get_unix_socket_path(_child_pid)),
              _unix_socket_file_path(get_unix_socket_file_path(_child_pid)),
              _child(std::move(child)),
              _output_stream(std::move(output_stream)) {}

    ~PythonUDFProcess() { shutdown(); }

    std::string get_uri() const { return _uri; }

    const std::string& get_socket_file_path() const { return _unix_socket_file_path; }

    bool is_shutdown() const { return _is_shutdown; }

    bool is_alive() const { return !_is_shutdown && _child.running(); }

    void remove_unix_socket();

    void shutdown();

    std::string to_string() const;

    pid_t get_child_pid() const { return _child_pid; }

    bool operator==(const PythonUDFProcess& other) const { return _child_pid == other._child_pid; }

    bool operator!=(const PythonUDFProcess& other) const { return !(*this == other); }

private:
    constexpr static int TERMINATE_RETRY_TIMES = 10;
    constexpr static size_t MAX_ACCUMULATED_LOG_SIZE = 65536;

    bool _is_shutdown {false};
    pid_t _child_pid;
    std::string _uri;
    std::string _unix_socket_file_path;
    mutable boost::process::child _child;
    boost::process::ipstream _output_stream;
    std::string _accumulated_log;
};

} // namespace doris