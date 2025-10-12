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

#include "python_udtf_executor.h"

#include <boost/process.hpp>
#include <memory>

#include "python_env.hpp"
#include "vec/functions/python/client/data_types/data_type_converter_factory.h"

namespace doris::pyudf {

Status PythonUdtfExecutor::setup(const std::string& python_script_path,
                                 const std::string& entry_function_name,
                                 const bool& null_on_failure) {
    python_udf_client = std::make_unique<PythonUdfClient>();
    RETURN_IF_ERROR(python_udf_client->launch(python_script_path, entry_function_name));
    this->null_on_failure = null_on_failure;
    this->python_script_path = python_script_path;
    this->entry_function_name = entry_function_name;
    return Status::OK();
}

Status PythonUdtfExecutor::init(vectorized::ColumnPtr udf_input_columns[],
                                int udf_input_column_number, int row_index,
                                const vectorized::DataTypes& udf_input_types,
                                vectorized::ColumnPtr& udf_output_column,
                                const vectorized::DataTypePtr& udf_output_type) const {
    if (udf_input_column_number <= 0) {
        return Status::InvalidArgument("The input column number must be positive.");
    }
    const auto output_converter = DataTypeConverterFactory::create_converter(udf_output_type);
    if (!output_converter) {
        return Status::InvalidArgument("Unsupported pyudf output type: {}.",
                                       udf_output_type->get_name());
    }
    RETURN_IF_ERROR(output_converter->create_column(udf_output_type, udf_output_column));
    ScopedPyObject input_args;
    {
        PythonGILGuard gil_guard;
        std::vector<PyObject*> py_objects;
        for (size_t arg_idx = 0; arg_idx < udf_input_column_number; ++arg_idx) {
            if (udf_input_columns[arg_idx]->is_null_at(row_index)) {
                current_tf_is_null = true;
                return Status::OK();
            }
            PyObject* py_object_arg;
            const auto input_converter =
                    DataTypeConverterFactory::create_converter(udf_input_types[arg_idx]);
            if (!input_converter) {
                return Status::InvalidArgument("Unsupported pyudf input type: {}.",
                                               udf_input_types[arg_idx]->get_name());
            }
            if (Status convert_result = input_converter->convert_to_python_object(
                        udf_input_columns[arg_idx], udf_input_types[arg_idx], row_index,
                        py_object_arg);
                !convert_result.ok()) {
                return convert_result;
            }
            py_objects.push_back(py_object_arg);
        }
        input_args.reset(PyTuple_New(udf_input_column_number));
        for (int index = 0; index < udf_input_column_number; ++index) {
            if (py_objects[index] == nullptr) {
                return Status::InternalError(
                        "The element of python udtf input tuple is null. udf_input_column_number "
                        "is {}, "
                        "element index is {}",
                        udf_input_column_number, index);
            }
            PyTuple_SetItem(input_args.get(), index, py_objects[index]);
        }
    }
    if (!input_args) {
        return Status::InternalError(
                "The python udtf input arg is null. udf_input_column_number is {}",
                udf_input_column_number);
    }
    RETURN_IF_ERROR(python_udf_client->start_python_udtf(input_args.get()));
    udtf_consumed = false;
    current_tf_is_null = false;
    return Status::OK();
}

Status PythonUdtfExecutor::fetch_next_batch_result(
        vectorized::ColumnPtr& udf_output_column,
        const vectorized::DataTypePtr& udf_output_type) const {
    // Skip this table function generation.
    if (udtf_consumed || current_tf_is_null) {
        return Status::OK();
    }
    const auto output_converter = DataTypeConverterFactory::create_converter(udf_output_type);
    if (!output_converter) {
        return Status::InvalidArgument("Unsupported pyudtf output type: {}.",
                                       udf_output_type->get_name());
    }
    PyObject* output_args = nullptr;
    Status status = python_udf_client->get_batch_udtf_result(output_args);
    if (!status.ok()) {
        if (null_on_failure) {
            // At this point, the Python UDF server process has already terminated (crashed or timed out),
            // so the existing client instance is no longer usable. We must create a new client and restart
            // the server process to ensure subsequent UDTF executions can proceed correctly.
            python_udf_client = std::make_unique<PythonUdfClient>();
            RETURN_IF_ERROR(python_udf_client->launch(python_script_path, entry_function_name));
            udtf_consumed = true;
            return Status::OK();
        }
        return status;
    }
    if (output_args) {
        PythonGILGuard gil_guard;
        RETURN_IF_ERROR(output_converter->flatten_to_column_data(output_args, udf_output_type,
                                                                 udf_output_column));
        Py_CLEAR(output_args);
    } else {
        udtf_consumed = true;
    }
    return Status::OK();
}

Status PythonUdtfExecutor::close() const {
    RETURN_IF_ERROR(python_udf_client->terminate());
    return Status::OK();
}

} // namespace doris::pyudf