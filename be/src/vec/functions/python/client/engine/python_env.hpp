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

#include <mutex>

namespace doris::pyudf {

/**
 * The {@link PythonEnv} is used to initialize the python environment.
 */
class PythonEnv {
public:
    PythonEnv(const PythonEnv&) = delete;
    PythonEnv& operator=(const PythonEnv&) = delete;

    /**
     * Initializes the Python environment. This method ensures that Py_Initialize() is called only once
     * in a multi-threaded environment.
     */
    static Status init_env() {
        static std::once_flag init_flag;
        std::call_once(init_flag, []() {
            // Init the env and create the gil.
            Py_Initialize();
            // Release the gil.
            PyEval_SaveThread();
            return Status::OK();
        });
        return Status::OK();
    }
};

/**
 * {@link PythonGILGuard} is a RAII-style class for managing the Global Interpreter Lock (GIL) in Python.
 *
 * This class ensures that the GIL is acquired upon construction and released upon destruction.
 */
class PythonGILGuard {
public:
    PythonGILGuard() { _gil_state = PyGILState_Ensure(); }
    ~PythonGILGuard() { PyGILState_Release(_gil_state); }

private:
    PyGILState_STATE _gil_state;
};

struct ClearPyObject {
    void operator()(PyObject* ptr) const {
        if (ptr != nullptr) {
            PythonGILGuard gil;
            Py_CLEAR(ptr);
        }
    }
};

// RAII wrapper for PyObject that automatically decrements refcount with GIL protection
typedef std::unique_ptr<PyObject, ClearPyObject> ScopedPyObject;

} // namespace doris::pyudf