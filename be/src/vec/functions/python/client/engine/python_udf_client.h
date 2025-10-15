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

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <common/status.h>

#include <boost/process.hpp>

#include "vec/functions/python/client/engine/python_env.hpp"
#include "vec/functions/python/client/engine/python_object_buffer_pool.h"
#include "vec/functions/python/common/interaction_protocol.h"
#include "vec/functions/python/common/interaction_channel.h"

namespace doris::pyudf {

/**
 * {@link PythonUdfClient} serves as a medium for the BE process to interact with a C++ subprocess.
 * The subprocess invokes Python UDF inside and avoids sharing the same GIL with the BE process,
 * establishing a client/server relationship between the BE and the subprocess.
 *
 * The client provides interfaces for communication with the Python UDF subprocess, allowing the
 * input of `PyObject` type data, invocation of specific functions within the Python code, and retrieval
 * of `PyObject` type computation results.
 *
 * Thread Safety: This class is not thread-safe and should only be used within a single thread.
 */
class PythonUdfClient {
public:
    PythonUdfClient() = default;
    PythonUdfClient(const PythonUdfClient&) = delete;
    PythonUdfClient& operator=(const PythonUdfClient&) = delete;

    /**
     * Launch the client with the specific python script path and function name.
     *
     * @param python_script_path the path to the Python script containing the UDF.
     * @param entry_python_filename the name of the entry Python script filename.
     * @return status.
     */
    Status launch(const std::string& python_script_path, const std::string& entry_python_filename);

    /**
     * Invoke the inner python udf.
     *
     * @param input input python object.
     * @param output output python object.
     * @return status.
     */
    Status invoke_python_udf(PyObject* input, ScopedPyObject& output) const;

    /**
     * Start the inner python udtf.
     *
     * @param input input python object.
     * @return status.
     */
    Status start_python_udtf(PyObject* input) const;

    /**
     * Get the result of python udtf in batch.
     *
     * @param output output python object.
     * @return status.
     */
    Status get_batch_udtf_result(PyObject*& output) const;

    /**
     * Terminate the python udf client.
     * @return status
     */
    Status terminate() const;

private:
    InteractionChannel _channel;
    boost::process::child _server;
    // This flag is used to indicate whether the client is started successfully.
    mutable bool server_started = false;

    /**
     * Check the status of the server.
     * @return status.
     */
    Status check_server_status() const;

    /**
     * Send the request to server.
     */
    Status send_request(const ClientRequest& request) const;
};

/**
 * {@link PythonUdfServerManager} is used to manage all server processes.
 */
class PythonUdfServerManager {
public:
    boost::process::group& get_server_process_group() { return _server_process_group; }

    /**
     * Terminate all illegal and residual server processes when start be.
     */
    void clean_at_beginning() {
        std::string command = "ps -e -o pid,comm | grep py_udf | awk '{print $1}' | xargs kill -9";
        std::system(command.c_str());
        // Fill the buffer pool when start be.
        PythonObjectBufferPool::instance();
    }

    /**
     * Terminate all illegal and residual server processes when stop be.
     */
    void clean_at_exit() { _server_process_group.terminate(); }

private:
    boost::process::group _server_process_group;
};

// Global instance of {@link PythonUdfServerManager}.
inline PythonUdfServerManager python_udf_server_manager;

} // namespace doris::pyudf
