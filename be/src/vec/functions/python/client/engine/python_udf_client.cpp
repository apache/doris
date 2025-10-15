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

#include "python_udf_client.h"

#include "python_env.hpp"
#include "python_object_buffer_pool.h"

namespace doris::pyudf {

Status PythonUdfClient::launch(const std::string& python_script_path,
                               const std::string& entry_python_filename) {
    try {
        LOG(INFO) << "The python udf/udtf file: " << entry_python_filename
                  << " path is : " << python_script_path;
        auto channel_info = create_channel_info_for_ipc();
        // Parent process uses pipe_req[1] for writing and pipe_resp[0] for reading
        // Child process uses pipe_req[0] for reading and pipe_resp[1] for writing
        InteractionChannel parent(channel_info.pipe_resp[0], channel_info.pipe_req[1]);
        if (!parent.is_valid()) {
            return Status::InternalError("Failed to create channel for python udf/udtf");
        }

        // Start the server process in the process group.
        boost::process::child server(
                std::string(std::getenv("DORIS_HOME")) + "/lib/python/doris_py_udf",
                boost::process::args({python_script_path, entry_python_filename,
                                      std::to_string(channel_info.pipe_req[0]),
                                      std::to_string(channel_info.pipe_req[1]),
                                      std::to_string(channel_info.pipe_resp[0]),
                                      std::to_string(channel_info.pipe_resp[1])}),
                python_udf_server_manager.get_server_process_group());

        this->_server = std::move(server);
        this->_channel = std::move(parent);
        // close fds for the child, otherwise this fd won't be closed when child dies.
        // Therefore, no EOF event and block forever.
        close(channel_info.pipe_req[0]);
        close(channel_info.pipe_resp[1]);
        server_started = true;
        return Status::OK();
    } catch (const std::runtime_error& e) {
        return Status::InternalError("Failed to launch python udf server: " +
                                     std::string(e.what()));
    }
}

Status PythonUdfClient::invoke_python_udf(PyObject* input, ScopedPyObject& output) const {
    // 1. Send the request to server.
    ClientRequest request(START_UDF);
    {
        PythonGILGuard gil_guard;
        request.writePythonObjectToPayload(input);
    }
    RETURN_IF_ERROR(send_request(request));
    // 2. Get the response.
    ServerResponse response;
    if (!_channel.recv(&response.status_code, sizeof(size_t))) {
        return Status::InternalError("Failed to read status_code");
    }
    if (!_channel.recv(&response.payload_length, sizeof(size_t))) {
        return Status::InternalError("Failed to read payload_length");
    }
    RETURN_IF_ERROR(check_server_status());
    PythonObjectBufferGuard bufferGuard(response.payload_length);
    if (!bufferGuard.get()) {
        return Status::InternalError("The size of python udf result is large than 64MB.");
    }
    if (!_channel.recv(bufferGuard.get()->data(), response.payload_length)) {
        return Status::InternalError("Failed to read payload");
    }
    response.payload = std::move(*bufferGuard.get());
    if (response.getStatusCode() == FAILURE) {
        RETURN_IF_ERROR(send_request(ClientRequest(TERMINATE)));
        if (!response.getPayload().empty()) {
            return Status::InternalError("Python UDF Exception: {}", response.getPayload());
        }
        return Status::InternalError("Python UDF failed for fatal errors");
    }
    if (response.getStatusCode() == TIMEOUT) {
        RETURN_IF_ERROR(terminate());
        return Status::InternalError(
                "Python UDF execution timeout for {} seconds. Automatically terminating the "
                "process.",
                MAX_EXECUTE_TIME_SECONDS);
    }
    // 3. Parse the python object from response.
    {
        PythonGILGuard gil_guard;
        output.reset(response.readPythonObjectFromPayload());
        if (!output) {
            return Status::InternalError("Failed to deserialize python udf result.");
        }
    }
    return Status::OK();
}

Status PythonUdfClient::start_python_udtf(PyObject* input) const {
    // Send the request to server.
    ClientRequest request(START_UDTF);
    {
        PythonGILGuard gil_guard;
        request.writePythonObjectToPayload(input);
    }
    RETURN_IF_ERROR(send_request(request));
    return Status::OK();
}

Status PythonUdfClient::get_batch_udtf_result(PyObject*& output) const {
    ServerResponse response;
    if (!_channel.recv(&response.status_code, sizeof(size_t))) {
        return Status::InternalError("Failed to read status_code");
    }
    if (!_channel.recv(&response.payload_length, sizeof(size_t))) {
        return Status::InternalError("Failed to read payload_length");
    }
    RETURN_IF_ERROR(check_server_status());
    PythonObjectBufferGuard bufferGuard(response.payload_length);
    if (!bufferGuard.get()) {
        return Status::InternalError("The size of python udtf result is large than 64MB.");
    }
    if (!_channel.recv(bufferGuard.get()->data(), response.payload_length)) {
        return Status::InternalError("Failed to read payload");
    }
    response.payload = std::move(*bufferGuard.get());
    if (response.getStatusCode() == BATCH_RESULT) {
        {
            PythonGILGuard gil_guard;
            output = response.readPythonObjectFromPayload();
            if (!output) {
                return Status::InternalError("Failed to deserialize python udtf result.");
            }
        }
        return Status::OK();
    }
    if (response.getStatusCode() == FAILURE) {
        PythonGILGuard gil_guard;
        Py_CLEAR(output);
        RETURN_IF_ERROR(send_request(ClientRequest(TERMINATE)));
        if (!response.getPayload().empty()) {
            return Status::InternalError("Python UDTF Exception: {}", response.getPayload());
        }
        return Status::InternalError("Python UDTF failed for fatal errors");
    }
    if (response.getStatusCode() == TIMEOUT) {
        PythonGILGuard gil_guard;
        Py_CLEAR(output);
        RETURN_IF_ERROR(terminate());
        return Status::InternalError(
                "Python UTDF execution timeout for {} seconds. Automatically terminating the "
                "process.",
                MAX_EXECUTE_TIME_SECONDS);
    }
    if (response.getStatusCode() != SUCCESS) {
        return Status::InternalError("The server response is illegal.");
    }
    return Status::OK();
}

Status PythonUdfClient::terminate() const {
    if (!server_started) {
        return Status::OK();
    }
    auto* _this = const_cast<PythonUdfClient*>(this);
    _this->_server.terminate();
    _this->_channel.release();
    server_started = false;
    return Status::OK();
}

Status PythonUdfClient::check_server_status() const {
    auto* _this = const_cast<PythonUdfClient*>(this);
    if (_this->_server.running()) {
        return Status::OK();
    }
    // The server is not running, possibly due to a fatal error. Terminate early to clean up
    // resources (like process handles and pipes) to avoid leaks. This is important to prevent
    // resource leaks and to ensure a clean state for subsequent operations or error handling.
    RETURN_IF_ERROR(terminate());
    return Status::InternalError(
            "The python udf/udtf encounters a fatal error and terminates unexpectedly.");
}

Status PythonUdfClient::send_request(const ClientRequest& request) const {
    RETURN_IF_ERROR(check_server_status());
    if (!_channel.send(&request.command_type, sizeof(size_t))) {
        return Status::InternalError("Failed to write status_code.");
    }
    if (!_channel.send(&request.payload_length, sizeof(size_t))) {
        return Status::InternalError("Failed to write payload_length.");
    }
    if (request.payload_length > 0) {
        if (request.payload.empty()) {
            return Status::InternalError("The request is illegal.");
        }
        if (!_channel.send(request.payload.c_str(), request.payload_length)) {
            return Status::InternalError("Failed to write payload.");
        }
    }
    return Status::OK();
}

} // namespace doris::pyudf
