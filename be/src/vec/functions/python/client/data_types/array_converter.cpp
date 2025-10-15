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

#include "array_converter.h"

#include <vec/columns/column_array.h>
#include <vec/data_types/data_type_array.h>
#include <vec/data_types/data_type_nullable.h>

namespace doris::pyudf {

Status ArrayConverter::convert_to_python_object(const vectorized::ColumnPtr& column,
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
    const auto& array_type = static_cast<const vectorized::DataTypeArray&>(*type);
    // 1. Get the actual data type of column.
    vectorized::DataTypePtr nested_type = array_type.get_nested_type();
    if (nested_type->is_nullable()) {
        nested_type = assert_cast<const vectorized::DataTypeNullable*>(nested_type.get())
                              ->get_nested_type();
    }
    const vectorized::WhichDataType which_nested_type(nested_type);
    const auto* array_column = vectorized::check_and_get_column<vectorized::ColumnArray>(source);
    const vectorized::ColumnArray::Offsets64& offsets = array_column->get_offsets();
    // 2. Get the nullable nested column if it's existed.
    vectorized::ColumnNullable* nullable_nested_column = nullptr;
    if (vectorized::is_column_nullable(*array_column->get_data_ptr().get())) {
        const auto* const_nullable_nested_column =
                reinterpret_cast<const vectorized::ColumnNullable*>(
                        array_column->get_data_ptr().get());
        nullable_nested_column =
                const_cast<vectorized::ColumnNullable*>(const_nullable_nested_column);
    }
    // 3. Convert the pyobject to column data.
    vectorized::ColumnPtr nested_column_ptr = remove_nullable(array_column->get_data_ptr());
    const size_t num_elements = offsets[row_index] - offsets[row_index - 1];
    py_object = PyList_New(0);
    for (size_t i = 0; i < num_elements; ++i) {
        const size_t elem_index = offsets[row_index - 1] + i;
        if (nullable_nested_column && nullable_nested_column->is_null_at(elem_index)) {
            return Status::InvalidArgument(
                    "Currently python udf doesn't support to "
                    "process NULL array element.");
        }
        PyObject* elem_py_object = nullptr;
        if (which_nested_type.is_int8()) {
            const auto* ground_truth =
                    vectorized::check_and_get_column<vectorized::ColumnInt8>(nested_column_ptr);
            elem_py_object = PyLong_FromLong(ground_truth->get_data()[elem_index]);
        } else if (which_nested_type.is_int16()) {
            const auto* ground_truth =
                    vectorized::check_and_get_column<vectorized::ColumnInt16>(nested_column_ptr);
            elem_py_object = PyLong_FromLong(ground_truth->get_data()[elem_index]);
        } else if (which_nested_type.is_int32()) {
            const auto* ground_truth =
                    vectorized::check_and_get_column<vectorized::ColumnInt32>(nested_column_ptr);
            elem_py_object = PyLong_FromLong(ground_truth->get_data()[elem_index]);
        } else if (which_nested_type.is_int64()) {
            const auto* ground_truth =
                    vectorized::check_and_get_column<vectorized::ColumnInt64>(nested_column_ptr);
            elem_py_object = PyLong_FromLong(ground_truth->get_data()[elem_index]);
        } else if (which_nested_type.is_float32()) {
            const auto* ground_truth =
                    vectorized::check_and_get_column<vectorized::ColumnFloat32>(nested_column_ptr);
            elem_py_object = PyFloat_FromDouble(ground_truth->get_data()[elem_index]);
        } else if (which_nested_type.is_float64()) {
            const auto* ground_truth =
                    vectorized::check_and_get_column<vectorized::ColumnFloat64>(nested_column_ptr);
            elem_py_object = PyFloat_FromDouble(ground_truth->get_data()[elem_index]);
        } else if (which_nested_type.is_string_or_fixed_string()) {
            const auto* ground_truth =
                    vectorized::check_and_get_column<vectorized::ColumnString>(nested_column_ptr);
            const std::string ground_truth_str = ground_truth->get_data_at(elem_index).to_string();
            const char* string_data = ground_truth_str.c_str();
            elem_py_object = PyUnicode_DecodeUTF8(string_data, strlen(string_data), nullptr);
            if (elem_py_object == nullptr) {
                return Status::InternalError("The string input type cannot be decoded by utf-8.");
            }
        } else if (which_nested_type.is_boolean()) {
            const auto* ground_truth =
                    vectorized::check_and_get_column<vectorized::ColumnBool>(nested_column_ptr);
            elem_py_object = PyBool_FromLong(ground_truth->get_data()[elem_index]);
        } else {
            Py_CLEAR(py_object);
            return Status::InvalidArgument(
                    fmt::format("Unsupported array element type: {}", nested_type->get_name()));
        }
        if (PyList_Append(py_object, elem_py_object) < 0) {
            Py_CLEAR(py_object);
            Py_CLEAR(elem_py_object);
            return Status::InternalError("Failed to append element to Python list.");
        }
        Py_CLEAR(elem_py_object);
    }
    return Status::OK();
}

Status ArrayConverter::convert_to_column_data(PyObject* py_object,
                                              const vectorized::DataTypePtr& type,
                                              vectorized::ColumnPtr& column) const {
    if (!PyList_Check(py_object)) {
        return Status::InvalidArgument(
                "The function out type is array but the actual "
                "python out type isn't Python List.");
    }
    // 1. Get the actual data type of column.
    const auto& array_type = static_cast<const vectorized::DataTypeArray&>(*type);
    vectorized::DataTypePtr nested_array_type = array_type.get_nested_type();
    if (nested_array_type->is_nullable()) {
        nested_array_type =
                assert_cast<const vectorized::DataTypeNullable*>(nested_array_type.get())
                        ->get_nested_type();
    }
    // 2. Insert data to columns.
    vectorized::ColumnPtr nested_temp_array_column;
    RETURN_IF_ERROR(generate_array_data_column(nested_array_type, nested_temp_array_column));
    vectorized::ColumnPtr temp_array_column = vectorized::ColumnNullable::create(
            nested_temp_array_column,
            vectorized::ColumnUInt8::create(nested_temp_array_column->size(), 0));
    const size_t list_size = PyList_Size(py_object);
    vectorized::Array array_data_field;
    for (size_t i = 0; i < list_size; ++i) {
        PyObject* py_element = PyList_GetItem(py_object, i);
        if (!py_element) {
            return Status::InternalError("Python udf failed to get item from Python list.");
        }
        RETURN_IF_ERROR(add_to_array_data_column(nested_array_type, temp_array_column, py_element));
        vectorized::Field array_field = (*temp_array_column)[i];
        array_data_field.push_back(array_field);
    }
    reinterpret_cast<vectorized::ColumnArray&>(*const_cast<vectorized::IColumn*>(column.get()))
            .insert(array_data_field);
    return Status::OK();
}

Status ArrayConverter::create_column(const vectorized::DataTypePtr& type,
                                     vectorized::ColumnPtr& column) const {
    const auto& array_type = static_cast<const vectorized::DataTypeArray&>(*type);
    vectorized::DataTypePtr nested_array_data_type = array_type.get_nested_type();
    if (nested_array_data_type->is_nullable()) {
        nested_array_data_type =
                assert_cast<const vectorized::DataTypeNullable*>(nested_array_data_type.get())
                        ->get_nested_type();
    }
    vectorized::ColumnPtr nested_array_column;
    RETURN_IF_ERROR(generate_array_data_column(nested_array_data_type, nested_array_column));
    auto nullable_float_col = vectorized::ColumnNullable::create(
            nested_array_column, vectorized::ColumnUInt8::create(nested_array_column->size(), 0));
    column = vectorized::ColumnArray::create(nullable_float_col,
                                             vectorized::ColumnArray::ColumnOffsets::create());
    return Status::OK();
}

Status ArrayConverter::insert_column_default_data(const vectorized::DataTypePtr& type,
                                                  vectorized::ColumnPtr& column) const {
    reinterpret_cast<vectorized::ColumnArray&>(*const_cast<vectorized::IColumn*>(column.get()))
            .insert_default();
    return Status::OK();
}

Status ArrayConverter::generate_array_data_column(const vectorized::DataTypePtr& type,
                                                  vectorized::ColumnPtr& column) {
    if (const vectorized::WhichDataType which_return_type(type); which_return_type.is_int8()) {
        column = vectorized::ColumnInt8::create();
    } else if (which_return_type.is_int16()) {
        column = vectorized::ColumnInt16::create();
    } else if (which_return_type.is_int32()) {
        column = vectorized::ColumnInt32::create();
    } else if (which_return_type.is_int64()) {
        column = vectorized::ColumnInt64::create();
    } else if (which_return_type.is_float32()) {
        column = vectorized::ColumnFloat32::create();
    } else if (which_return_type.is_float64()) {
        column = vectorized::ColumnFloat64::create();
    } else if (which_return_type.is_string_or_fixed_string()) {
        column = vectorized::ColumnString::create();
    } else if (which_return_type.is_boolean()) {
        column = vectorized::ColumnBool::create();
    } else {
        return Status::InvalidArgument("Unsupported python udf array element type: {}",
                                       type->get_name());
    }
    return Status::OK();
}

Status ArrayConverter::add_to_array_data_column(vectorized::DataTypePtr& type,
                                                vectorized::ColumnPtr& column,
                                                PyObject* py_element) {
    const vectorized::WhichDataType which_type(type);
    bool type_conflict = false;
    if (which_type.is_int8()) {
        if (!PyLong_Check(py_element)) {
            type_conflict = true;
        } else {
            reinterpret_cast<vectorized::ColumnNullable&>(
                    *const_cast<vectorized::IColumn*>(column.get()))
                    .insert(PyLong_AsLong(py_element));
        }
    } else if (which_type.is_int16()) {
        if (!PyLong_Check(py_element)) {
            type_conflict = true;
        } else {
            reinterpret_cast<vectorized::ColumnNullable&>(
                    *const_cast<vectorized::IColumn*>(column.get()))
                    .insert(PyLong_AsLong(py_element));
        }
    } else if (which_type.is_int32()) {
        if (!PyLong_Check(py_element)) {
            type_conflict = true;
        } else {
            reinterpret_cast<vectorized::ColumnNullable&>(
                    *const_cast<vectorized::IColumn*>(column.get()))
                    .insert(PyLong_AsLong(py_element));
        }
    } else if (which_type.is_int64()) {
        if (!PyLong_Check(py_element)) {
            type_conflict = true;
        } else {
            reinterpret_cast<vectorized::ColumnNullable&>(
                    *const_cast<vectorized::IColumn*>(column.get()))
                    .insert(PyLong_AsLong(py_element));
        }
    } else if (which_type.is_float32()) {
        if (!PyFloat_Check(py_element)) {
            type_conflict = true;
        } else {
            reinterpret_cast<vectorized::ColumnNullable&>(
                    *const_cast<vectorized::IColumn*>(column.get()))
                    .insert(PyFloat_AsDouble(py_element));
        }
    } else if (which_type.is_float64()) {
        if (!PyFloat_Check(py_element)) {
            type_conflict = true;
        } else {
            reinterpret_cast<vectorized::ColumnNullable&>(
                    *const_cast<vectorized::IColumn*>(column.get()))
                    .insert(PyFloat_AsDouble(py_element));
        }
    } else if (which_type.is_string_or_fixed_string()) {
        if (!PyUnicode_Check(py_element)) {
            type_conflict = true;
        } else {
            const char* row_string = PyUnicode_AsUTF8(py_element);
            reinterpret_cast<vectorized::ColumnNullable&>(
                    *const_cast<vectorized::IColumn*>(column.get()))
                    .insert(std::string(row_string));
        }
    } else if (which_type.is_boolean()) {
        if (!PyBool_Check(py_element)) {
            type_conflict = true;
        } else {
            reinterpret_cast<vectorized::ColumnNullable&>(
                    *const_cast<vectorized::IColumn*>(column.get()))
                    .insert(Py_IsTrue(py_element));
        }
    } else {
        return Status::InvalidArgument(
                "Currently the defined python udf array element type is not supported : "
                "{}.",
                type->get_name());
    }
    if (type_conflict) {
        if (PyObject* py_type = PyObject_Type(py_element); py_type) {
            if (PyObject* py_type_str = PyObject_Str(py_type); py_type_str) {
                if (const char* type_name = PyUnicode_AsUTF8(py_type_str); type_name) {
                    return Status::InvalidArgument(
                            "Error: Python UDF returned an array with a type mismatch. "
                            "Expected element type {}, but found element type {}",
                            type->get_name(), type_name);
                }
                Py_CLEAR(py_type_str);
            }
            Py_CLEAR(py_type);
        }
        return Status::InvalidArgument(
                "The actual return type of python udf is "
                "not same with function definition.");
    }
    return Status::OK();
}

Status ArrayConverter::flatten_to_column_data(PyObject* py_object,
                                              const vectorized::DataTypePtr& type,
                                              vectorized::ColumnPtr& column) const {
    if (!PyList_Check(py_object)) {
        Py_CLEAR(py_object);
        return Status::InvalidArgument(
                "The function out type is array<array> but the actual "
                "python out type isn't Python List<List>.");
    }
    // 1. Get the actual data type of column.
    const auto& array_type = static_cast<const vectorized::DataTypeArray&>(*type);
    vectorized::DataTypePtr nested_array_type = array_type.get_nested_type();
    if (nested_array_type->is_nullable()) {
        nested_array_type =
                assert_cast<const vectorized::DataTypeNullable*>(nested_array_type.get())
                        ->get_nested_type();
    }
    // 2. Insert data to columns.
    const size_t list_size = PyList_Size(py_object);
    for (size_t i = 0; i < list_size; ++i) {
        PyObject* py_list_element = PyList_GetItem(py_object, i);
        if (!PyList_Check(py_list_element)) {
            return Status::InvalidArgument(
                    "The function out type is array<array> but the actual "
                    "python out list element isn't Python List.");
        }
        const size_t list_element_size = PyList_Size(py_list_element);
        vectorized::Array array_data_field;
        vectorized::ColumnPtr nested_temp_array_column;
        RETURN_IF_ERROR(generate_array_data_column(nested_array_type, nested_temp_array_column));
        vectorized::ColumnPtr temp_array_column = vectorized::ColumnNullable::create(
                nested_temp_array_column,
                vectorized::ColumnUInt8::create(nested_temp_array_column->size(), 0));
        for (size_t j = 0; j < list_element_size; ++j) {
            PyObject* py_element = PyList_GetItem(py_list_element, j);
            if (!py_element) {
                return Status::InternalError("Python udf failed to get item from Python list.");
            }
            RETURN_IF_ERROR(
                    add_to_array_data_column(nested_array_type, temp_array_column, py_element));
            vectorized::Field array_field = (*temp_array_column)[j];
            array_data_field.push_back(array_field);
        }
        reinterpret_cast<vectorized::ColumnArray&>(*const_cast<vectorized::IColumn*>(column.get()))
                .insert(array_data_field);
    }
    return Status::OK();
}

Status ArrayConverter::replicate_value_to_result_column(vectorized::ColumnPtr& source_column,
                                                        vectorized::MutableColumnPtr& result_column,
                                                        size_t index, size_t repeat_time) const {
    const auto array_data_field = reinterpret_cast<vectorized::ColumnArray&>(
            *const_cast<vectorized::IColumn*>(source_column.get()))[index];
    for (int i = 0; i < repeat_time; ++i) {
        reinterpret_cast<vectorized::ColumnArray&>(*(result_column.get())).insert(array_data_field);
    }
    return Status::OK();
}

Status ArrayConverter::copy_range_to_result_column(vectorized::ColumnPtr& source_column,
                                                   vectorized::MutableColumnPtr& result_column,
                                                   size_t start_index, size_t range_size) const {
    auto& target = reinterpret_cast<vectorized::ColumnArray&>(*result_column.get());
    int inc = 0;
    while (inc < range_size) {
        const auto array_data_field = reinterpret_cast<vectorized::ColumnArray&>(
                *const_cast<vectorized::IColumn*>(source_column.get()))[start_index + inc];
        target.insert(array_data_field);
        inc++;
    }
    return Status::OK();
}

} // namespace doris::pyudf