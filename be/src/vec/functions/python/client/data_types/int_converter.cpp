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

#include "int_converter.h"

namespace doris::pyudf {

Status TinyIntConverter::convert_to_python_object(const vectorized::ColumnPtr& column,
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
    const auto* ground_truth = vectorized::check_and_get_column<vectorized::ColumnInt8>(source);
    py_object = PyLong_FromLong(ground_truth->get_data()[row_index]);
    return Status::OK();
}

Status TinyIntConverter::convert_to_column_data(PyObject* py_object,
                                                const vectorized::DataTypePtr& type,
                                                vectorized::ColumnPtr& column) const {
    if (!PyLong_Check(py_object)) {
        return Status::InvalidArgument("Expected Python int for TINYINT output type.");
    }
    reinterpret_cast<vectorized::ColumnInt8&>(*const_cast<vectorized::IColumn*>(column.get()))
            .insert_value(PyLong_AsLong(py_object));
    return Status::OK();
}

Status TinyIntConverter::create_column(const vectorized::DataTypePtr& type,
                                       vectorized::ColumnPtr& column) const {
    column = vectorized::ColumnInt8::create();
    return Status::OK();
}

Status TinyIntConverter::insert_column_default_data(const vectorized::DataTypePtr& type,
                                                    vectorized::ColumnPtr& column) const {
    reinterpret_cast<vectorized::ColumnInt8&>(*const_cast<vectorized::IColumn*>(column.get()))
            .insert_default();
    return Status::OK();
}

Status TinyIntConverter::flatten_to_column_data(PyObject* py_object,
                                                const vectorized::DataTypePtr& type,
                                                vectorized::ColumnPtr& column) const {
    if (!PyList_Check(py_object)) {
        Py_CLEAR(py_object);
        return Status::InvalidArgument("Expected Python list for ARRAY<TINYINT> output type.");
    }
    const ssize_t list_size = PyList_Size(py_object);
    for (ssize_t index = 0; index < list_size; ++index) {
        PyObject* item = PyList_GET_ITEM(py_object, index);
        if (!PyLong_Check(item)) {
            return Status::InvalidArgument(
                    "Expected int element of Python list for ARRAY<TINYINT> output type.");
        }
        reinterpret_cast<vectorized::ColumnInt8&>(*const_cast<vectorized::IColumn*>(column.get()))
                .insert_value(PyLong_AsLong(item));
    }
    return Status::OK();
}

Status TinyIntConverter::replicate_value_to_result_column(
        vectorized::ColumnPtr& source_column, vectorized::MutableColumnPtr& result_column,
        size_t index, size_t repeat_time) const {
    const vectorized::Int8 result = reinterpret_cast<vectorized::ColumnInt8&>(
                                         *const_cast<vectorized::IColumn*>(source_column.get()))
                                         .get_element(index);
    reinterpret_cast<vectorized::ColumnInt8&>(*result_column.get())
            .insert_many_vals(result, repeat_time);
    return Status::OK();
}

Status TinyIntConverter::copy_range_to_result_column(vectorized::ColumnPtr& source_column,
                                                     vectorized::MutableColumnPtr& result_column,
                                                     size_t start_index, size_t range_size) const {
    auto& target = reinterpret_cast<vectorized::ColumnInt8&>(*result_column.get());
    const auto origin_size = target.size();
    target.resize(origin_size + range_size);
    int inc = 0;
    while (inc < range_size) {
        const auto result = reinterpret_cast<vectorized::ColumnInt8&>(
                                    *const_cast<vectorized::IColumn*>(source_column.get()))
                                    .get_element(start_index + inc);
        target.get_data()[origin_size + inc] = result;
        inc++;
    }
    return Status::OK();
}

Status SmallIntConverter::convert_to_python_object(const vectorized::ColumnPtr& column,
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
    const auto* ground_truth = vectorized::check_and_get_column<vectorized::ColumnInt16>(source);
    py_object = PyLong_FromLong(ground_truth->get_data()[row_index]);
    return Status::OK();
}

Status SmallIntConverter::convert_to_column_data(PyObject* py_object,
                                                 const vectorized::DataTypePtr& type,
                                                 vectorized::ColumnPtr& column) const {
    if (!PyLong_Check(py_object)) {
        Py_CLEAR(py_object);
        return Status::InvalidArgument("Expected Python int for SMALLINT output type.");
    }
    reinterpret_cast<vectorized::ColumnInt16&>(*const_cast<vectorized::IColumn*>(column.get()))
            .insert_value(PyLong_AsLong(py_object));
    return Status::OK();
}

Status SmallIntConverter::create_column(const vectorized::DataTypePtr& type,
                                        vectorized::ColumnPtr& column) const {
    column = vectorized::ColumnInt16::create();
    return Status::OK();
}

Status SmallIntConverter::insert_column_default_data(const vectorized::DataTypePtr& type,
                                                     vectorized::ColumnPtr& column) const {
    reinterpret_cast<vectorized::ColumnInt16&>(*const_cast<vectorized::IColumn*>(column.get()))
            .insert_default();
    return Status::OK();
}

Status SmallIntConverter::flatten_to_column_data(PyObject* py_object,
                                                 const vectorized::DataTypePtr& type,
                                                 vectorized::ColumnPtr& column) const {
    if (!PyList_Check(py_object)) {
        Py_CLEAR(py_object);
        return Status::InvalidArgument("Expected Python list for ARRAY<SMALLINT> output type.");
    }
    ssize_t list_size = PyList_Size(py_object);
    for (ssize_t index = 0; index < list_size; ++index) {
        PyObject* item = PyList_GET_ITEM(py_object, index);
        if (!PyLong_Check(item)) {
            return Status::InvalidArgument(
                    "Expected int element of Python list for ARRAY<SMALLINT> output type.");
        }
        reinterpret_cast<vectorized::ColumnInt16&>(*const_cast<vectorized::IColumn*>(column.get()))
                .insert_value(PyLong_AsLong(item));
    }
    return Status::OK();
}

Status SmallIntConverter::replicate_value_to_result_column(
        vectorized::ColumnPtr& source_column, vectorized::MutableColumnPtr& result_column,
        size_t index, size_t repeat_time) const {
    const vectorized::Int16 result = reinterpret_cast<vectorized::ColumnInt16&>(
                                        *const_cast<vectorized::IColumn*>(source_column.get()))
                                        .get_element(index);
    reinterpret_cast<vectorized::ColumnInt16&>(*result_column.get())
            .insert_many_vals(result, repeat_time);
    return Status::OK();
}

Status SmallIntConverter::copy_range_to_result_column(vectorized::ColumnPtr& source_column,
                                                      vectorized::MutableColumnPtr& result_column,
                                                      size_t start_index, size_t range_size) const {
    auto& target = reinterpret_cast<vectorized::ColumnInt16&>(*result_column.get());
    const auto origin_size = target.size();
    target.resize(origin_size + range_size);
    int inc = 0;
    while (inc < range_size) {
        const auto result = reinterpret_cast<vectorized::ColumnInt16&>(
                                    *const_cast<vectorized::IColumn*>(source_column.get()))
                                    .get_element(start_index + inc);
        target.get_data()[origin_size + inc] = result;
        inc++;
    }
    return Status::OK();
}

Status IntConverter::convert_to_python_object(const vectorized::ColumnPtr& column,
                                              const vectorized::DataTypePtr& type, size_t row_index,
                                              PyObject*& py_object) const {
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
    const auto* ground_truth = vectorized::check_and_get_column<vectorized::ColumnInt32>(source);
    py_object = PyLong_FromLong(ground_truth->get_data()[row_index]);
    return Status::OK();
}

Status IntConverter::convert_to_column_data(PyObject* py_object,
                                            const vectorized::DataTypePtr& type,
                                            vectorized::ColumnPtr& column) const {
    if (!PyLong_Check(py_object)) {
        Py_CLEAR(py_object);
        return Status::InvalidArgument("Expected Python int for INT output type.");
    }
    reinterpret_cast<vectorized::ColumnInt32&>(*const_cast<vectorized::IColumn*>(column.get()))
            .insert_value(PyLong_AsLong(py_object));
    return Status::OK();
}

Status IntConverter::create_column(const vectorized::DataTypePtr& type,
                                   vectorized::ColumnPtr& column) const {
    column = vectorized::ColumnInt32::create();
    return Status::OK();
}

Status IntConverter::insert_column_default_data(const vectorized::DataTypePtr& type,
                                                vectorized::ColumnPtr& column) const {
    reinterpret_cast<vectorized::ColumnInt32&>(*const_cast<vectorized::IColumn*>(column.get()))
            .insert_default();
    return Status::OK();
}

Status IntConverter::flatten_to_column_data(PyObject* py_object,
                                            const vectorized::DataTypePtr& type,
                                            vectorized::ColumnPtr& column) const {
    if (!PyList_Check(py_object)) {
        Py_CLEAR(py_object);
        return Status::InvalidArgument("Expected Python list for ARRAY<INT> output type.");
    }
    ssize_t list_size = PyList_Size(py_object);
    for (ssize_t index = 0; index < list_size; ++index) {
        PyObject* item = PyList_GET_ITEM(py_object, index);
        if (!PyLong_Check(item)) {
            return Status::InvalidArgument(
                    "Expected int element of Python list for ARRAY<INT> output type.");
        }
        reinterpret_cast<vectorized::ColumnInt32&>(*const_cast<vectorized::IColumn*>(column.get()))
                .insert_value(PyLong_AsLong(item));
    }
    return Status::OK();
}

Status IntConverter::replicate_value_to_result_column(vectorized::ColumnPtr& source_column,
                                                      vectorized::MutableColumnPtr& result_column,
                                                      size_t index, size_t repeat_time) const {
    const vectorized::Int32 result = reinterpret_cast<vectorized::ColumnInt32&>(
                                             *const_cast<vectorized::IColumn*>(source_column.get()))
                                             .get_element(index);
    reinterpret_cast<vectorized::ColumnInt32&>(*result_column.get())
            .insert_many_vals(result, repeat_time);
    return Status::OK();
}

Status IntConverter::copy_range_to_result_column(vectorized::ColumnPtr& source_column,
                                                 vectorized::MutableColumnPtr& result_column,
                                                 size_t start_index, size_t range_size) const {
    auto& target = reinterpret_cast<vectorized::ColumnInt32&>(*result_column.get());
    const auto origin_size = target.size();
    target.resize(origin_size + range_size);
    int inc = 0;
    while (inc < range_size) {
        const auto result = reinterpret_cast<vectorized::ColumnInt32&>(
                                    *const_cast<vectorized::IColumn*>(source_column.get()))
                                    .get_element(start_index + inc);
        target.get_data()[origin_size + inc] = result;
        inc++;
    }
    return Status::OK();
}

Status BigIntConverter::convert_to_python_object(const vectorized::ColumnPtr& column,
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
    const auto* ground_truth = vectorized::check_and_get_column<vectorized::ColumnInt64>(source);
    py_object = PyLong_FromLong(ground_truth->get_data()[row_index]);
    return Status::OK();
}

Status BigIntConverter::convert_to_column_data(PyObject* py_object,
                                               const vectorized::DataTypePtr& type,
                                               vectorized::ColumnPtr& column) const {
    if (!PyLong_Check(py_object)) {
        Py_CLEAR(py_object);
        return Status::InvalidArgument("Expected Python int for BIGINT output type.");
    }
    reinterpret_cast<vectorized::ColumnInt64&>(*const_cast<vectorized::IColumn*>(column.get()))
            .insert_value(PyLong_AsLong(py_object));
    return Status::OK();
}

Status BigIntConverter::create_column(const vectorized::DataTypePtr& type,
                                      vectorized::ColumnPtr& column) const {
    column = vectorized::ColumnInt64::create();
    return Status::OK();
}

Status BigIntConverter::insert_column_default_data(const vectorized::DataTypePtr& type,
                                                   vectorized::ColumnPtr& column) const {
    reinterpret_cast<vectorized::ColumnInt64&>(*const_cast<vectorized::IColumn*>(column.get()))
            .insert_default();
    return Status::OK();
}

Status BigIntConverter::flatten_to_column_data(PyObject* py_object,
                                               const vectorized::DataTypePtr& type,
                                               vectorized::ColumnPtr& column) const {
    if (!PyList_Check(py_object)) {
        Py_CLEAR(py_object);
        return Status::InvalidArgument("Expected Python list for ARRAY<BIGINT> output type.");
    }
    const ssize_t list_size = PyList_Size(py_object);
    for (ssize_t index = 0; index < list_size; ++index) {
        PyObject* item = PyList_GET_ITEM(py_object, index);
        if (!PyLong_Check(item)) {
            return Status::InvalidArgument(
                    "Expected int element of Python list for ARRAY<BIGINT> output type.");
        }
        reinterpret_cast<vectorized::ColumnInt64&>(*const_cast<vectorized::IColumn*>(column.get()))
                .insert_value(PyLong_AsLong(item));
    }
    return Status::OK();
}

Status BigIntConverter::replicate_value_to_result_column(
        vectorized::ColumnPtr& source_column, vectorized::MutableColumnPtr& result_column,
        size_t index, size_t repeat_time) const {
    const vectorized::Int64 result = reinterpret_cast<vectorized::ColumnInt64&>(
                                             *const_cast<vectorized::IColumn*>(source_column.get()))
                                             .get_element(index);
    reinterpret_cast<vectorized::ColumnInt64&>(*result_column.get())
            .insert_many_vals(result, repeat_time);
    return Status::OK();
}

Status BigIntConverter::copy_range_to_result_column(vectorized::ColumnPtr& source_column,
                                                    vectorized::MutableColumnPtr& result_column,
                                                    size_t start_index, size_t range_size) const {
    auto& target = reinterpret_cast<vectorized::ColumnInt64&>(*result_column.get());
    const auto origin_size = target.size();
    target.resize(origin_size + range_size);
    int inc = 0;
    while (inc < range_size) {
        const auto result = reinterpret_cast<vectorized::ColumnInt64&>(
                                    *const_cast<vectorized::IColumn*>(source_column.get()))
                                    .get_element(start_index + inc);
        target.get_data()[origin_size + inc] = result;
        inc++;
    }
    return Status::OK();
}

} // namespace doris::pyudf