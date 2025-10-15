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

#include "python_udf_executor.h"

#include <boost/process.hpp>
#include <memory>

#include "python_env.hpp"
#include "vec/functions/python/client/data_types/data_type_converter_factory.h"

namespace doris::pyudf {

Status PythonUdfExecutor::setup(const std::string& python_script_path,
                                const std::string& entry_python_filename,
                                const bool& null_on_failure) {
    python_udf_client = std::make_unique<PythonUdfClient>();
    RETURN_IF_ERROR(python_udf_client->launch(python_script_path, entry_python_filename));
    this->null_on_failure = null_on_failure;
    this->python_script_path = python_script_path;
    this->entry_python_filename = entry_python_filename;
    return Status::OK();
}

Status PythonUdfExecutor::execute(vectorized::ColumnPtr udf_input_columns[],
                                  int udf_input_column_number,
                                  const vectorized::DataTypes& udf_input_types,
                                  vectorized::ColumnPtr& udf_output_column,
                                  const vectorized::DataTypePtr& udf_output_type,
                                  vectorized::ColumnUInt8::MutablePtr& null_map) const {
    const auto output_converter = DataTypeConverterFactory::create_converter(udf_output_type);
    if (!output_converter) {
        return Status::InvalidArgument("Unsupported pyudf output type: {}.",
                                       udf_output_type->get_name());
    }
    RETURN_IF_ERROR(output_converter->create_column(udf_output_type, udf_output_column));

    std::vector<std::unique_ptr<DataTypeConverter>> input_converters(udf_input_column_number);
    for (int i = 0; i < udf_input_column_number; ++i) {
        auto input_converter = DataTypeConverterFactory::create_converter(udf_input_types[i]);
        if (!input_converter) {
            return Status::InvalidArgument("Unsupported pyudf input type: {}.",
                                           udf_input_types[i]->get_name());
        }
        input_converters[i] = std::move(input_converter);
    }

    const int row_size = udf_input_columns[0]->size();
    for (size_t row_index = 0; row_index < row_size; ++row_index) {
        bool is_null_result = false;
        ScopedPyObject input_args;
        {
            PythonGILGuard gil_guard;
            // python objs in py_objects will be placed into input_args tuple and transfer their ref
            // to input_args. These object will be gc once Py_CLEAR(input_args) is called.
            std::vector<ScopedPyObject> py_objects;
            for (size_t arg_idx = 0; arg_idx < udf_input_column_number; ++arg_idx) {
                if (udf_input_columns[arg_idx]->is_null_at(row_index)) {
                    is_null_result = true;
                    break;
                }
                PyObject* py_object_arg;
                RETURN_IF_ERROR(input_converters[arg_idx]->convert_to_python_object(
                        udf_input_columns[arg_idx], udf_input_types[arg_idx], row_index,
                        py_object_arg));
                py_objects.emplace_back(py_object_arg);
            }
            if (!is_null_result) {
                input_args.reset(PyTuple_New(udf_input_column_number));
                for (int index = 0; index < udf_input_column_number; ++index) {
                    PyTuple_SetItem(input_args.get(), index, py_objects[index].release());
                }
            }
        }
        if (is_null_result) {
            RETURN_IF_ERROR(output_converter->insert_column_default_data(udf_output_type,
                                                                         udf_output_column));
            null_map->get_data().push_back(1);
            continue;
        }
        ScopedPyObject output_args;
        if (Status status = python_udf_client->invoke_python_udf(input_args.get(), output_args);
            !status.ok()) {
            if (null_on_failure) {
                // At this point, the Python UDF server process has already terminated (crashed or timed out),
                // so the existing client instance is no longer usable. We must create a new client and restart
                // the server process to ensure subsequent UDF executions can proceed correctly.
                python_udf_client = std::make_unique<PythonUdfClient>();
                RETURN_IF_ERROR(
                        python_udf_client->launch(python_script_path, entry_python_filename));
                RETURN_IF_ERROR(output_converter->insert_column_default_data(udf_output_type,
                                                                             udf_output_column));
                null_map->get_data().push_back(1);
                continue;
            }
            return status;
        }
        {
            PythonGILGuard gil_guard;
            if (Py_IsNone(output_args.get())) {
                RETURN_IF_ERROR(output_converter->insert_column_default_data(udf_output_type,
                                                                             udf_output_column));
                null_map->get_data().push_back(1);
            } else {
                RETURN_IF_ERROR(output_converter->convert_to_column_data(
                        output_args.get(), udf_output_type, udf_output_column));
                null_map->get_data().push_back(0);
            }
        }
    }
    return Status::OK();
}

Status PythonUdfExecutor::batch_execute(vectorized::ColumnPtr udf_input_columns[],
                                        int udf_input_column_number,
                                        const vectorized::DataTypes& udf_input_types,
                                        int batch_size,
                                        vectorized::ColumnPtr& udf_output_column,
                                        const vectorized::DataTypePtr& udf_output_type,
                                        vectorized::ColumnUInt8::MutablePtr& null_map) const {
    const auto output_converter = DataTypeConverterFactory::create_converter(udf_output_type);
    if (!output_converter) {
        return Status::InvalidArgument("Unsupported pyudf output type: {}.",
                                       udf_output_type->get_name());
    }
    RETURN_IF_ERROR(output_converter->create_column(udf_output_type, udf_output_column));

    std::vector<std::unique_ptr<DataTypeConverter>> input_converters(udf_input_column_number);
    for (int i = 0; i < udf_input_column_number; ++i) {
        auto input_converter = DataTypeConverterFactory::create_converter(udf_input_types[i]);
        if (!input_converter) {
            return Status::InvalidArgument("Unsupported pyudf input type: {}.",
                                           udf_input_types[i]->get_name());
        }
        input_converters[i] = std::move(input_converter);
    }

    std::vector<ScopedPyObject> batch_reqs;
    const int row_size = udf_input_columns[0]->size();
    for (size_t row_index = 0; row_index < row_size; ++row_index) {
        bool is_null_result = false;
        {
            PythonGILGuard gil_guard;
            std::vector<ScopedPyObject> py_objects(udf_input_column_number);
            for (size_t arg_idx = 0; arg_idx < udf_input_column_number; ++arg_idx) {
                if (udf_input_columns[arg_idx]->is_null_at(row_index)) {
                    is_null_result = true;
                    break;
                }
                PyObject* py_object_arg;
                RETURN_IF_ERROR(input_converters[arg_idx]->convert_to_python_object(
                        udf_input_columns[arg_idx], udf_input_types[arg_idx], row_index,
                        py_object_arg));
                py_objects[arg_idx].reset(py_object_arg);
            }
            if (!is_null_result) {
                PyObject* row_obj = PyTuple_New(udf_input_column_number);
                for (int index = 0; index < udf_input_column_number; ++index) {
                    PyTuple_SetItem(row_obj, index, py_objects[index].release());
                }
                batch_reqs.emplace_back(row_obj);
            }
        }

        if (!batch_reqs.empty() && (batch_reqs.size() >= batch_size || is_null_result || row_index+1 >= row_size)) {
            // invoke python and insert batch results
            RETURN_IF_ERROR(batch_invoke_udf(batch_reqs, output_converter,
                                             udf_output_column, udf_output_type, null_map));
            batch_reqs.clear();
        }

        if (is_null_result) {
            // handle the former error result
            RETURN_IF_ERROR(output_converter->insert_column_default_data(udf_output_type,
                                                                         udf_output_column));
            null_map->get_data().push_back(1);
        }
    }
    return Status::OK();
}

Status PythonUdfExecutor::close() const {
    RETURN_IF_ERROR(python_udf_client->terminate());
    return Status::OK();
}

Status PythonUdfExecutor::batch_invoke_udf(std::vector<ScopedPyObject>& batch_reqs,
                                           const std::unique_ptr<DataTypeConverter>& output_converter,
                                           vectorized::ColumnPtr& udf_output_column,
                                           const vectorized::DataTypePtr& udf_output_type,
                                           vectorized::ColumnUInt8::MutablePtr& null_map) const {
    const int batch_size = batch_reqs.size();
    ScopedPyObject input_args;
    {
        PythonGILGuard gil_guard;
        PyObject* list = PyList_New(0);
        for (auto & batch_req : batch_reqs) {
            PyList_Append(list, batch_req.release());
        }
        input_args.reset(PyTuple_New(1));
        PyTuple_SetItem(input_args.get(), 0, list);
    }

    ScopedPyObject output_args;
    Status status = python_udf_client->invoke_python_udf(input_args.get(), output_args);
    if (!status.ok()) {
        if (!null_on_failure) {
            return status;
        }
        // At this point, the Python UDF server process has already terminated (crashed or timed out),
        // so the existing client instance is no longer usable. We must create a new client and restart
        // the server process to ensure subsequent UDF executions can proceed correctly.
        python_udf_client = std::make_unique<PythonUdfClient>();
        RETURN_IF_ERROR(
                python_udf_client->launch(python_script_path, entry_python_filename));
        for (int i = 0; i < batch_size; ++i) {
            RETURN_IF_ERROR(output_converter->insert_column_default_data(udf_output_type,
                                                                         udf_output_column));
            null_map->get_data().push_back(1);
        }
    }

    {
        PythonGILGuard gil_guard;
        RETURN_IF_ERROR(convert_batch_outputs(output_args.get(), batch_size,
                                              output_converter, udf_output_column, udf_output_type,
                                              null_map));
    }
    return Status::OK();
}

Status PythonUdfExecutor::convert_batch_outputs(PyObject* output_obj,
                                                int batch_size,
                                                const std::unique_ptr<DataTypeConverter>& output_converter,
                                                vectorized::ColumnPtr& udf_output_column,
                                                const vectorized::DataTypePtr& udf_output_type,
                                                vectorized::ColumnUInt8::MutablePtr& null_map) const {
    if (!PyList_Check(output_obj)) {
        return Status::InvalidArgument(
                "The function out type is array but the actual "
                "python out type isn't Python List.");
    }
    const size_t list_size = PyList_Size(output_obj);
    if (list_size != batch_size) {
        return Status::InvalidArgument(
                "Python UDF input batch size={} doesn't match output size={}.",
                batch_size, list_size);
    }

    for (size_t i = 0; i < list_size; ++i) {
        PyObject* py_element = PyList_GetItem(output_obj, i);
        if (!py_element) {
            return Status::InternalError("Python udf failed to get item from Python list.");
        }
        if (Py_IsNone(py_element)) {
            RETURN_IF_ERROR(output_converter->insert_column_default_data(udf_output_type,
                                                                         udf_output_column));
            null_map->get_data().push_back(1);
        } else {
            RETURN_IF_ERROR(output_converter->convert_to_column_data(py_element, udf_output_type,
                                                                     udf_output_column));
            null_map->get_data().push_back(0);
        }
    }

    return Status::OK();
}

} // namespace doris::pyudf