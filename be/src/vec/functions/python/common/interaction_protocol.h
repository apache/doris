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

#include <Python.h>

#include "marshal.h"

namespace doris::pyudf {

/**
 * {@link CommandType} enumerates the types of commands that can be sent from the client to the server.
 *
 * This enumeration defines the possible command types that the client can send to the server,
 * such as starting a new operation or terminating the process.
 */
enum CommandType {
    START_UDF = 0,
    START_UDTF = 1,
    TERMINATE = 2,
};

/**
 * {@link StatusCode} enumerates the status codes that can be sent from the server to the client.
 *
 * This enumeration defines the possible status codes that the server can send to the client
 * in response to a request, indicating success, failure, or timeout.
 */
enum StatusCode {
    // common code
    SUCCESS = 0,
    FAILURE = 1,
    TIMEOUT = 2,
    // only for udtf
    BATCH_RESULT = 3
};

/**
 * {@link ClientRequest} represents a request sent from the client to the server.
 *
 * This structure encapsulates the command type, payload length, and payload data
 * that are sent from the client to the server during communication.
 */
struct ClientRequest {
    size_t command_type;
    size_t payload_length {0};
    std::string payload;

    explicit ClientRequest() : command_type(static_cast<size_t>(TERMINATE)) {}

    explicit ClientRequest(const CommandType command)
            : command_type(static_cast<size_t>(command)) {}

    CommandType getCommandType() const {
        if (command_type > 2 || command_type < 0) {
            return TERMINATE;
        }
        return static_cast<CommandType>(command_type);
    }

    size_t getPayloadLength() const { return payload_length; }

    PyObject* readPythonObjectFromPayload() const {
        if (payload.empty() || payload_length <= 0) {
            return nullptr;
        }
        return PyMarshal_ReadObjectFromString(payload.c_str(), payload_length);
    }

    void writePythonObjectToPayload(PyObject* py_object) {
        PyObject* serialized_object = PyMarshal_WriteObjectToString(py_object, Py_MARSHAL_VERSION);
        const size_t serialized_object_size = PyBytes_Size(serialized_object);
        const char* serialized_object_data = PyBytes_AsString(serialized_object);
        this->payload = std::string(serialized_object_data, serialized_object_size);
        this->payload_length = serialized_object_size;
        Py_CLEAR(serialized_object);
    }
};

/**
 * {@link ServerResponse} represents a response sent from the server to the client.
 *
 * This structure encapsulates the status code, payload length, and payload data
 * that are sent from the server to the client in response to a request.
 */
struct ServerResponse {
    size_t status_code;
    size_t payload_length;
    std::string payload;

    explicit ServerResponse() : status_code(static_cast<size_t>(FAILURE)), payload_length(0) {}

    explicit ServerResponse(const StatusCode status)
            : status_code(static_cast<size_t>(status)), payload_length(0) {}

    explicit ServerResponse(const StatusCode code, const std::string& data)
            : status_code(static_cast<size_t>(code)), payload_length(data.size()) {
        this->payload = data;
    }

    StatusCode getStatusCode() const {
        if (status_code > 3 || status_code < 0) {
            return FAILURE;
        }
        return static_cast<StatusCode>(status_code);
    }

    size_t getPayloadLength() const { return payload_length; }

    std::string& getPayload() { return payload; }

    PyObject* readPythonObjectFromPayload() const {
        if (payload.empty() || payload_length <= 0) {
            return nullptr;
        }
        return PyMarshal_ReadObjectFromString(payload.c_str(), payload_length);
    }

    void writePythonObjectToPayload(PyObject* py_object) {
        PyObject* serialized_object = PyMarshal_WriteObjectToString(py_object, Py_MARSHAL_VERSION);
        const size_t serialized_object_size = PyBytes_Size(serialized_object);
        const char* serialized_object_data = PyBytes_AsString(serialized_object);
        this->payload = std::string(serialized_object_data, serialized_object_size);
        this->payload_length = serialized_object_size;
        Py_CLEAR(serialized_object);
    }
};

// The maximum allowed execution time for Python UDF to process a single data item (3 hours).
constexpr int MAX_EXECUTE_TIME_SECONDS = 3600 * 3;

} // namespace doris::pyudf
