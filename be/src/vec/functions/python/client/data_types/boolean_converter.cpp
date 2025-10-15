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

#include "boolean_converter.h"

namespace doris::pyudf {

Status BooleanConverter::convert_to_python_object(const vectorized::ColumnPtr& column,
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
    const auto* ground_truth = vectorized::check_and_get_column<vectorized::ColumnBool>(source);
    py_object = PyBool_FromLong(ground_truth->get_data()[row_index]);
    return Status::OK();
}

Status BooleanConverter::convert_to_column_data(PyObject* py_object,
                                                const vectorized::DataTypePtr& type,
                                                vectorized::ColumnPtr& column) const {
    if (!PyBool_Check(py_object)) {
        return Status::InvalidArgument("Expected Python bool for BOOLEAN output type.");
    }
    reinterpret_cast<vectorized::ColumnBool&>(*const_cast<vectorized::IColumn*>(column.get()))
            .insert_value(Py_IsTrue(py_object));
    return Status::OK();
}

Status BooleanConverter::create_column(const vectorized::DataTypePtr& type,
                                       vectorized::ColumnPtr& column) const {
    column = vectorized::ColumnBool::create();
    return Status::OK();
}

Status BooleanConverter::insert_column_default_data(const vectorized::DataTypePtr& type,
                                                    vectorized::ColumnPtr& column) const {
    reinterpret_cast<vectorized::ColumnBool&>(*const_cast<vectorized::IColumn*>(column.get()))
            .insert_default();
    return Status::OK();
}

Status BooleanConverter::flatten_to_column_data(PyObject* py_object,
                                                const vectorized::DataTypePtr& type,
                                                vectorized::ColumnPtr& column) const {
    if (!PyList_Check(py_object)) {
        Py_CLEAR(py_object);
        return Status::InvalidArgument("Expected Python list for ARRAY<BOOLEAN> output type.");
    }
    const ssize_t list_size = PyList_Size(py_object);
    for (ssize_t index = 0; index < list_size; ++index) {
        const PyObject* item = PyList_GET_ITEM(py_object, index);
        if (!PyBool_Check(item)) {
            return Status::InvalidArgument(
                    "Expected bool element of Python list for ARRAY<BOOLEAN> output type.");
        }
        reinterpret_cast<vectorized::ColumnBool&>(*const_cast<vectorized::IColumn*>(column.get()))
                .insert_value(Py_IsTrue(item));
    }
    return Status::OK();
}

Status BooleanConverter::replicate_value_to_result_column(
        vectorized::ColumnPtr& source_column, vectorized::MutableColumnPtr& result_column,
        size_t index, size_t repeat_time) const {
    const vectorized::UInt8 result = reinterpret_cast<vectorized::ColumnBool&>(
                                             *const_cast<vectorized::IColumn*>(source_column.get()))
                                             .get_element(index);
    reinterpret_cast<vectorized::ColumnBool&>(*result_column.get())
            .insert_many_vals(result, repeat_time);
    return Status::OK();
}

Status BooleanConverter::copy_range_to_result_column(vectorized::ColumnPtr& source_column,
                                                   vectorized::MutableColumnPtr& result_column,
                                                   size_t start_index, size_t range_size) const {
    auto& target = reinterpret_cast<vectorized::ColumnBool&>(*result_column.get());
    const auto origin_size = target.size();
    target.resize(origin_size + range_size);
    int inc = 0;
    while (inc < range_size) {
        const auto result = reinterpret_cast<vectorized::ColumnBool&>(
                                    *const_cast<vectorized::IColumn*>(source_column.get()))
                                    .get_element(start_index + inc);
        target.get_data()[origin_size + inc] = result;
        inc++;
    }
    return Status::OK();
}

} // namespace doris::pyudf