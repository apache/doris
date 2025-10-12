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

#include "message_handler.h"

void send_python_error_response() {
    // 1. Fetch the current Python error information.
    PyObject *ptype, *pvalue, *ptraceback;
    PyErr_Fetch(&ptype, &pvalue, &ptraceback);
    PyErr_NormalizeException(&ptype, &pvalue, &ptraceback);
    // 2. Convert the error type and value to strings.
    PyObject* type_str = PyObject_Str(ptype);
    const char* type_message = PyUnicode_AsUTF8(type_str);
    PyObject* value_str = PyObject_Str(pvalue);
    const char* value_message = PyUnicode_AsUTF8(value_str);
    // 3. Format the traceback if available.
    PyObject* traceback_str = nullptr;
    if (ptraceback) {
        if (PyObject* traceback_module = PyImport_ImportModule("traceback"); traceback_module) {
            if (PyObject* format_exception =
                        PyObject_GetAttrString(traceback_module, "format_exception");
                format_exception && PyCallable_Check(format_exception)) {
                if (PyObject* traceback_list = PyObject_CallFunctionObjArgs(
                            format_exception, ptype, pvalue, ptraceback, NULL);
                    traceback_list) {
                    traceback_str = PyUnicode_Join(PyUnicode_FromString(""), traceback_list);
                }
            }
        }
    }
    // 4. Construct the full error message.
    std::string full_error_message = "\nError Type: " + std::string(type_message) +
                                     "\nError Message: " + std::string(value_message);
    if (traceback_str) {
        const char* traceback_message = PyUnicode_AsUTF8(traceback_str);
        full_error_message += "\n";
        full_error_message += traceback_message;
    }
    // 5. Send the response
    const doris::pyudf::ServerResponse response(doris::pyudf::FAILURE, full_error_message);
    send_response(response);
    // 6. Clean up.
    Py_CLEAR(type_str);
    Py_CLEAR(value_str);
    Py_CLEAR(traceback_str);
    Py_CLEAR(ptype);
    Py_CLEAR(pvalue);
    Py_CLEAR(ptraceback);
}

void send_custom_error_response(const std::string& error_message) {
    const doris::pyudf::ServerResponse response(doris::pyudf::FAILURE, error_message);
    send_response(response);
}
