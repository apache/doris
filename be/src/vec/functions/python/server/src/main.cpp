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

#include <unistd.h>
#include "python_udf_server.h"

InteractionChannel channel;

/**
 * The main entry point for the Python UDF server executable. It initializes the Python environment,
 * starts the execution monitor, and enters a loop to receive requests, execute udf/udtf, and send
 * responses.
 */
int main(int argc, char* argv[]) {
    if (argc < 7) {
        std::cerr << "This server is meant to be launched only by doris_be" << std::endl;
        return 1;
    }
    const std::string script_path(argv[1]);
    const std::string module_name(argv[2]);
    const int pipe_req_read_fd = std::stoi(argv[3]);
    const int pipe_req_write_fd = std::stoi(argv[4]);
    const int pipe_resp_read_fd = std::stoi(argv[5]);
    const int pipe_resp_write_fd = std::stoi(argv[6]);
    if (pipe_req_read_fd < 3 || pipe_req_write_fd < 3 || pipe_resp_read_fd < 3 || pipe_resp_write_fd < 3) {
        std::cerr << "Invalid file descriptor: should not be stdin, stdout, or stderr" << std::endl;
        return 1;
    }
    // close fds for the parent, otherwise this fd won't be closed when parent dies.
    // Therefore, no EOF event and block forever.
    close(pipe_req_write_fd);
    close(pipe_resp_read_fd);

    channel = InteractionChannel(pipe_req_read_fd, pipe_resp_write_fd);
    initialize(script_path);
    ExecutionMonitor monitor;
    monitor.start();
    try {
        while (true) {
            doris::pyudf::ClientRequest request;
            get_request(request);
            if (request.getCommandType() == doris::pyudf::TERMINATE) {
                clean_up();
                return 0;
            }
            monitor.start_execution();
            if (request.getCommandType() == doris::pyudf::START_UDF) {
                execute_udf(module_name, request);
            }
            if (request.getCommandType() == doris::pyudf::START_UDTF) {
                execute_udtf(module_name, request);
            }
            monitor.end_execution();
        }
    } catch (const std::exception& ex) {
        std::cerr << "PythonUDF Server Exception: " << ex.what() << std::endl;
        return 1;
    }
}
