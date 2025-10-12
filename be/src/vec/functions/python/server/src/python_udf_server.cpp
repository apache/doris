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
#include "python_udf_server.h"

#include <filesystem>
#include <regex>

struct ClearPyObject {
    void operator()(PyObject* ptr) const {
        if (ptr != nullptr) {
            // No need to hold GIL since server doesn't have multiple threads
            Py_CLEAR(ptr);
        }
    }
};

typedef std::unique_ptr<PyObject, ClearPyObject> ScopedPyObject;

PyObject* invoke_evaluate_method(const std::string& module_name, PyObject* input_arg) {
    // The python module and function will be load only once at the first time.
    static PyObject* function = nullptr;
    if (!function) {
        PyObject* module = PyImport_ImportModule(module_name.c_str());
        if (!module) {
            send_python_error_response();
            return nullptr;
        }
        function = PyObject_GetAttrString(module, "evaluate");
        if (!function) {
            send_python_error_response();
            return nullptr;
        }
    }
    PyObject* output_results = PyObject_CallObject(function, input_arg);
    return output_results;
}

void execute_udf(const std::string& module_name, doris::pyudf::ClientRequest request) {
    ScopedPyObject input_arg(request.readPythonObjectFromPayload());
    if (!input_arg) {
        send_custom_error_response("The client request is illegal.");
        return;
    }
    ScopedPyObject output_results(invoke_evaluate_method(module_name, input_arg.get()));
    if (output_results) {
        doris::pyudf::ServerResponse response(doris::pyudf::SUCCESS);
        response.writePythonObjectToPayload(output_results.get());
        send_response(response);
    } else {
        send_python_error_response();
    }
}

void execute_udtf(const std::string& module_name, doris::pyudf::ClientRequest request) {
    ScopedPyObject input_arg(request.readPythonObjectFromPayload());
    if (!input_arg) {
        send_custom_error_response("The client request is illegal.");
        return;
    }
    ScopedPyObject output_results(invoke_evaluate_method(module_name, input_arg.get()));
    if (output_results) {
        if (!PyIter_Check(output_results.get())) {
            send_custom_error_response(
                    "The Python UDTF should generate results using the 'yield' keyword "
                    "instead of returning a single result.");
            return;
        }
        ScopedPyObject batch_results(PyList_New(0));
        if (!batch_results) {
            send_python_error_response();
            return;
        }
        int batch_count = 0;
        while (true) {
            PyObject* next_result = PyIter_Next(output_results.get());
            if (!next_result) {
                if (PyErr_Occurred()) {
                    send_python_error_response();
                    return;
                }
                break;
            }
            if (PyList_Append(batch_results.get(), next_result) < 0) {
                send_python_error_response();
                break;
            }
            Py_CLEAR(next_result);
            batch_count++;
            if (batch_count >= UDTF_BATCH_SIZE) {
                doris::pyudf::ServerResponse response(doris::pyudf::BATCH_RESULT);
                response.writePythonObjectToPayload(batch_results.get());
                send_response(response);

                batch_results.reset(PyList_New(0));
                if (!batch_results) {
                    send_python_error_response();
                    break;
                }
                batch_count = 0;
            }
        }
        if (PyList_Size(batch_results.get()) > 0) {
            doris::pyudf::ServerResponse response(doris::pyudf::BATCH_RESULT);
            response.writePythonObjectToPayload(batch_results.get());
            send_response(response);
        }
        doris::pyudf::ServerResponse response(doris::pyudf::SUCCESS);
        send_response(response);
    } else {
        send_python_error_response();
    }
}

std::string get_python_version(const std::string& base_path) {
    if (!std::filesystem::exists(base_path) || !std::filesystem::is_directory(base_path)) {
        return "";
    }
    std::regex version_pattern("python(\\d+)\\.(\\d+)");
    std::smatch match;
    for (const auto& entry : std::filesystem::directory_iterator(base_path)) {
        if (entry.is_directory()) {
            std::string filename = entry.path().filename().string();
            if (std::regex_search(filename, match, version_pattern)) {
                return match[0];
            }
        }
    }
    return "";
}

void initialize(const std::string& script_path) {
    Py_Initialize();
    std::ostringstream oss;
    std::string python_version = get_python_version(script_path + "/my_workspace/lib/");
    oss << "import sys; sys.path.append('" << script_path << "'); ";
    if (!python_version.empty()) {
        oss << "sys.path.append('" << script_path << "/my_workspace/lib/" << python_version
            << "/site-packages');";
    }
    const std::string import_script = oss.str();
    PyRun_SimpleString(import_script.c_str());
}

/**
 * Clean up the python environment.
 */
void clean_up() {
    Py_Finalize();
    channel.release();
}
