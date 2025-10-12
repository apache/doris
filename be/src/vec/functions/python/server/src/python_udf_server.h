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

#include <string>

#include "message_handler.h"

/**
 * Invoke the `evaluate` method in the user python scripts with the given input arguments.
 *
 * @param module_name the user script module name.
 * @param input_arg the input python object.
 */
PyObject* invoke_evaluate_method(const std::string& module_name, PyObject* input_arg);

/**
 * Executes user-defined function.
 *
 * @param module_name the user script module name.
 * @param request the client request.
 */
void execute_udf(const std::string& module_name, doris::pyudf::ClientRequest request);

/**
 * Executes user-defined table function.
 *
 * @param module_name the user script module name.
 * @param request the client request.
 */
void execute_udtf(const std::string& module_name, doris::pyudf::ClientRequest request);

/**
 * Retrieves the python version from the directory structure, which is used to append the
 * correct site-packages directory to the python path.
 *
 * @param base_path the bash directory of the path to the user python script.
 * @return the python version string.
 */
std::string get_python_version(const std::string& base_path);

/**
 * Initializes the python environment and sets up the python path to include the script
 * directory and the python version specific site-packages directory.
 *
 * @param script_path the path to the python script.
 */
void initialize(const std::string& script_path);

/**
 * Clean up the python environment.
 */
void clean_up();
