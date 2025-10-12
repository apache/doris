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

#include "string_converter.h"

namespace doris::pyudf {

Status StringConverter::convert_to_python_object(const vectorized::ColumnPtr& column,
                                                 const vectorized::DataTypePtr& type,
                                                 size_t row_index, PyObject*& py_object) const {
    vectorized::ColumnPtr source = column;
    bool is_null = false;
    if (column->is_nullable()) {
        is_null = reinterpret_cast<const vectorized::ColumnNullable*>(column.get())
                          ->is_null_at(row_index);
        source = remove_nullable(column);
    }
    if (is_null) {
        return Status::InvalidArgument("Currently python udf doesn't support to process NULL.");
    }
    const auto* ground_truth = vectorized::check_and_get_column<vectorized::ColumnString>(source);
    const std::string ground_truth_str = ground_truth->get_data_at(row_index).to_string();
    const char* string_data = ground_truth_str.c_str();
    py_object = PyUnicode_DecodeUTF8(string_data, strlen(string_data), nullptr);
    if (py_object == nullptr) {
        return Status::InternalError("The input string of pyudf cannot be decoded by utf-8.");
    }
    return Status::OK();
}

Status StringConverter::convert_to_column_data(PyObject* py_object,
                                               const vectorized::DataTypePtr& type,
                                               vectorized::ColumnPtr& column) const {
    if (!PyUnicode_Check(py_object)) {
        return Status::InvalidArgument(
                "Expected Python string for CHAR/VARCHAR/STRING output type.");
    }
    const char* row_string = PyUnicode_AsUTF8(py_object);
    reinterpret_cast<vectorized::ColumnString&>(*const_cast<vectorized::IColumn*>(column.get()))
            .insert_data(row_string, strlen(row_string));
    return Status::OK();
}

Status StringConverter::create_column(const vectorized::DataTypePtr& type,
                                      vectorized::ColumnPtr& column) const {
    column = vectorized::ColumnString::create();
    return Status::OK();
}

Status StringConverter::insert_column_default_data(const vectorized::DataTypePtr& type,
                                                   vectorized::ColumnPtr& column) const {
    reinterpret_cast<vectorized::ColumnString&>(*const_cast<vectorized::IColumn*>(column.get()))
            .insert_default();
    return Status::OK();
}

Status StringConverter::flatten_to_column_data(PyObject* py_object,
                                               const vectorized::DataTypePtr& type,
                                               vectorized::ColumnPtr& column) const {
    if (!PyList_Check(py_object)) {
        Py_CLEAR(py_object);
        return Status::InvalidArgument(
                "Expected Python list for ARRAY<CHAR/VARCHAR/STRING> output type.");
    }
    const ssize_t list_size = PyList_Size(py_object);
    for (ssize_t index = 0; index < list_size; ++index) {
        PyObject* item = PyList_GET_ITEM(py_object, index);
        if (!PyUnicode_Check(item)) {
            return Status::InvalidArgument(
                    "Expected string element of Python list for ARRAY<CHAR/VARCHAR/STRING> "
                    "output type.");
        }
        const char* row_string = PyUnicode_AsUTF8(item);
        reinterpret_cast<vectorized::ColumnString&>(*const_cast<vectorized::IColumn*>(column.get()))
                .insert_data(row_string, strlen(row_string));
    }
    return Status::OK();
}

Status StringConverter::replicate_value_to_result_column(
        vectorized::ColumnPtr& source_column, vectorized::MutableColumnPtr& result_column,
        size_t index, size_t repeat_time) const {
    const auto* repeated_data =
            vectorized::check_and_get_column<vectorized::ColumnString>(source_column);
    const std::string repeated_data_str = repeated_data->get_data_at(index).to_string();
    for (int time = 0; time < repeat_time; ++time) {
        reinterpret_cast<vectorized::ColumnString&>(*result_column.get())
                .insert_data(repeated_data_str.c_str(), repeated_data_str.size());
    }
    return Status::OK();
}

Status StringConverter::copy_range_to_result_column(vectorized::ColumnPtr& source_column,
                                                    vectorized::MutableColumnPtr& result_column,
                                                    size_t start_index, size_t range_size) const {
    auto& target = reinterpret_cast<vectorized::ColumnString&>(*result_column.get());
    int inc = 0;
    while (inc < range_size) {
        const std::string column_str =
                vectorized::check_and_get_column<vectorized::ColumnString>(source_column)
                        ->get_data_at(start_index + inc)
                        .to_string();
        target.insert_data(column_str.c_str(), column_str.size());
        inc++;
    }
    return Status::OK();
}

} // namespace doris::pyudf