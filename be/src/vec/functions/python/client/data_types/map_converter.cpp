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

#include "map_converter.h"

namespace doris::pyudf {

Status MapConverter::convert_to_python_object(const vectorized::ColumnPtr& column,
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
    const auto& map_type = static_cast<const vectorized::DataTypeMap&>(*type);
    // 1. Get key type.
    const vectorized::DataTypePtr key_type = map_type.get_key_type();
    vectorized::DataTypePtr nested_key_type = key_type;
    if (nested_key_type->is_nullable()) {
        nested_key_type = assert_cast<const vectorized::DataTypeNullable*>(nested_key_type.get())
                                  ->get_nested_type();
    }
    // 2. Get value type.
    const vectorized::DataTypePtr value_type = map_type.get_value_type();
    vectorized::DataTypePtr nested_value_type = value_type;
    if (nested_value_type->is_nullable()) {
        nested_value_type =
                assert_cast<const vectorized::DataTypeNullable*>(nested_value_type.get())
                        ->get_nested_type();
    }
    const auto* map_column = vectorized::check_and_get_column<vectorized::ColumnMap>(source);
    // 3. Get key and value column.
    const vectorized::ColumnPtr nested_key_ptr = remove_nullable(map_column->get_keys_ptr());
    const vectorized::ColumnPtr nested_value_ptr = remove_nullable(map_column->get_values_ptr());
    const vectorized::ColumnMap::Offsets64& offsets = map_column->get_offsets();
    const size_t num_elements = offsets[row_index] - offsets[row_index - 1];
    py_object = PyDict_New();
    for (size_t i = 0; i < num_elements; ++i) {
        const size_t elem_index = offsets[row_index - 1] + i;
        PyObject* key_py_object = nullptr;
        PyObject* value_py_object = nullptr;
        // 4. Convert key.
        if (const Status key_status = convert_key_or_value_column_to_python_object(
                    nested_key_ptr, nested_key_type, elem_index, key_py_object);
            !key_status.ok()) {
            Py_CLEAR(py_object);
            return key_status;
        }
        // 5. Convert value.
        if (const Status value_status = convert_key_or_value_column_to_python_object(
                    nested_value_ptr, nested_value_type, elem_index, value_py_object);
            !value_status.ok()) {
            Py_CLEAR(py_object);
            Py_CLEAR(key_py_object);
            return value_status;
        }
        // 6. Add key and value to dictionary.
        if (PyDict_SetItem(py_object, key_py_object, value_py_object) < 0) {
            Py_CLEAR(py_object);
            Py_CLEAR(key_py_object);
            Py_CLEAR(value_py_object);
            return Status::InternalError("Failed to set item in Python dictionary.");
        }
        Py_CLEAR(key_py_object);
        Py_CLEAR(value_py_object);
    }
    return Status::OK();
}

Status MapConverter::convert_to_column_data(PyObject* py_object,
                                            const vectorized::DataTypePtr& type,
                                            vectorized::ColumnPtr& column) const {
    if (!PyDict_Check(py_object)) {
        return Status::InvalidArgument(
                "The function out type is map but the actual "
                "python out type isn't Python Dict.");
    }
    const auto& map_type = static_cast<const vectorized::DataTypeMap&>(*type);
    // 1. Get key type.
    const vectorized::DataTypePtr key_type = map_type.get_key_type();
    vectorized::DataTypePtr nested_key_type = key_type;
    if (nested_key_type->is_nullable()) {
        nested_key_type = assert_cast<const vectorized::DataTypeNullable*>(nested_key_type.get())
                                  ->get_nested_type();
    }
    // 2. Get value type.
    const vectorized::DataTypePtr value_type = map_type.get_value_type();
    vectorized::DataTypePtr nested_value_type = value_type;
    if (nested_value_type->is_nullable()) {
        nested_value_type =
                assert_cast<const vectorized::DataTypeNullable*>(nested_value_type.get())
                        ->get_nested_type();
    }
    // 3. Create temporary columns for keys and values.
    vectorized::ColumnPtr nested_temp_keys_column;
    vectorized::ColumnPtr nested_temp_values_column;
    RETURN_IF_ERROR(generate_key_or_value_data_column(nested_key_type, nested_temp_keys_column));
    RETURN_IF_ERROR(
            generate_key_or_value_data_column(nested_value_type, nested_temp_values_column));
    vectorized::ColumnPtr temp_keyes_column = vectorized::ColumnNullable::create(
            nested_temp_keys_column,
            vectorized::ColumnUInt8::create(nested_temp_keys_column->size(), 0));
    vectorized::ColumnPtr temp_values_column = vectorized::ColumnNullable::create(
            nested_temp_values_column,
            vectorized::ColumnUInt8::create(nested_temp_values_column->size(), 0));
    PyObject *key, *value;
    Py_ssize_t pos = 0;
    while (PyDict_Next(py_object, &pos, &key, &value)) {
        RETURN_IF_ERROR(add_to_key_or_value_column(nested_key_type, temp_keyes_column, key));
        RETURN_IF_ERROR(add_to_key_or_value_column(nested_value_type, temp_values_column, value));
    }
    // 4. Insert the temporary columns into the target column.
    vectorized::Array key_array_field;
    vectorized::Array value_array_field;
    for (size_t i = 0; i < temp_keyes_column->size(); ++i) {
        vectorized::Field key_field = (*temp_keyes_column)[i];
        vectorized::Field value_field = (*temp_values_column)[i];
        key_array_field.push_back(key_field);
        value_array_field.push_back(value_field);
    }
    reinterpret_cast<vectorized::ColumnMap&>(*const_cast<vectorized::IColumn*>(column.get()))
            .insert(vectorized::Map {key_array_field, value_array_field});
    return Status::OK();
}

Status MapConverter::create_column(const vectorized::DataTypePtr& type,
                                   vectorized::ColumnPtr& column) const {
    const auto& map_type = static_cast<const vectorized::DataTypeMap&>(*type);
    const vectorized::DataTypePtr key_type = map_type.get_key_type();
    const vectorized::DataTypePtr value_type = map_type.get_value_type();
    vectorized::ColumnPtr nested_keys_column;
    vectorized::ColumnPtr nested_values_column;
    const vectorized::DataTypePtr nested_key_type = remove_nullable(key_type);
    const vectorized::DataTypePtr nested_value_type = remove_nullable(value_type);
    RETURN_IF_ERROR(generate_key_or_value_data_column(nested_key_type, nested_keys_column));
    RETURN_IF_ERROR(generate_key_or_value_data_column(nested_value_type, nested_values_column));
    const vectorized::ColumnPtr keys_column = vectorized::ColumnNullable::create(
            nested_keys_column, vectorized::ColumnUInt8::create(nested_keys_column->size(), 0));
    const vectorized::ColumnPtr values_column = vectorized::ColumnNullable::create(
            nested_values_column, vectorized::ColumnUInt8::create(nested_values_column->size(), 0));
    column = vectorized::ColumnMap::create(keys_column, values_column,
                                           vectorized::ColumnMap::COffsets::create());
    return Status::OK();
}

Status MapConverter::insert_column_default_data(const vectorized::DataTypePtr& type,
                                                vectorized::ColumnPtr& column) const {
    reinterpret_cast<vectorized::ColumnMap&>(*const_cast<vectorized::IColumn*>(column.get()))
            .insert_default();
    return Status::OK();
}

Status MapConverter::generate_key_or_value_data_column(const vectorized::DataTypePtr& type,
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
        return Status::InvalidArgument("Unsupported python udf map key or value element type: {}",
                                       type->get_name());
    }
    return Status::OK();
}

Status MapConverter::convert_key_or_value_column_to_python_object(
        const vectorized::ColumnPtr& column, const vectorized::DataTypePtr& type, size_t row_index,
        PyObject*& py_object) {
    const vectorized::WhichDataType which_type(type);
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
    if (which_type.is_int8()) {
        const auto* ground_truth = vectorized::check_and_get_column<vectorized::ColumnInt8>(source);
        py_object = PyLong_FromLong(ground_truth->get_data()[row_index]);
    } else if (which_type.is_int16()) {
        const auto* ground_truth =
                vectorized::check_and_get_column<vectorized::ColumnInt16>(source);
        py_object = PyLong_FromLong(ground_truth->get_data()[row_index]);
    } else if (which_type.is_int32()) {
        const auto* ground_truth =
                vectorized::check_and_get_column<vectorized::ColumnInt32>(source);
        py_object = PyLong_FromLong(ground_truth->get_data()[row_index]);
    } else if (which_type.is_int64()) {
        const auto* ground_truth =
                vectorized::check_and_get_column<vectorized::ColumnInt64>(source);
        py_object = PyLong_FromLong(ground_truth->get_data()[row_index]);
    } else if (which_type.is_float32()) {
        const auto* ground_truth =
                vectorized::check_and_get_column<vectorized::ColumnFloat32>(source);
        py_object = PyFloat_FromDouble(ground_truth->get_data()[row_index]);
    } else if (which_type.is_float64()) {
        const auto* ground_truth =
                vectorized::check_and_get_column<vectorized::ColumnFloat64>(source);
        py_object = PyFloat_FromDouble(ground_truth->get_data()[row_index]);
    } else if (which_type.is_string_or_fixed_string()) {
        const auto* ground_truth =
                vectorized::check_and_get_column<vectorized::ColumnString>(source);
        const std::string ground_truth_str = ground_truth->get_data_at(row_index).to_string();
        const char* string_data = ground_truth_str.c_str();
        py_object = PyUnicode_DecodeUTF8(string_data, strlen(string_data), nullptr);
        if (py_object == nullptr) {
            return Status::InternalError("The string input type cannot be decoded by utf-8.");
        }
    } else if (which_type.is_boolean()) {
        const auto* ground_truth = vectorized::check_and_get_column<vectorized::ColumnBool>(source);
        py_object = PyBool_FromLong(ground_truth->get_data()[row_index]);
    } else {
        return Status::InvalidArgument(
                "Unsupported python udf input key "
                "or value of map type: {}",
                type->get_name());
    }
    return Status::OK();
}

Status MapConverter::add_to_key_or_value_column(vectorized::DataTypePtr& type,
                                                vectorized::ColumnPtr& column,
                                                PyObject* key_or_value_element) {
    const vectorized::WhichDataType which_type(type);
    bool type_conflict = false;
    if (which_type.is_int8()) {
        if (!PyLong_Check(key_or_value_element)) {
            type_conflict = true;
        } else {
            reinterpret_cast<vectorized::ColumnNullable&>(
                    *const_cast<vectorized::IColumn*>(column.get()))
                    .insert(PyLong_AsLong(key_or_value_element));
        }
    } else if (which_type.is_int16()) {
        if (!PyLong_Check(key_or_value_element)) {
            type_conflict = true;
        } else {
            reinterpret_cast<vectorized::ColumnNullable&>(
                    *const_cast<vectorized::IColumn*>(column.get()))
                    .insert(PyLong_AsLong(key_or_value_element));
        }
    } else if (which_type.is_int32()) {
        if (!PyLong_Check(key_or_value_element)) {
            type_conflict = true;
        } else {
            reinterpret_cast<vectorized::ColumnNullable&>(
                    *const_cast<vectorized::IColumn*>(column.get()))
                    .insert(PyLong_AsLong(key_or_value_element));
        }
    } else if (which_type.is_int64()) {
        if (!PyLong_Check(key_or_value_element)) {
            type_conflict = true;
        } else {
            reinterpret_cast<vectorized::ColumnNullable&>(
                    *const_cast<vectorized::IColumn*>(column.get()))
                    .insert(PyLong_AsLong(key_or_value_element));
        }
    } else if (which_type.is_float32()) {
        if (!PyFloat_Check(key_or_value_element)) {
            type_conflict = true;
        } else {
            reinterpret_cast<vectorized::ColumnNullable&>(
                    *const_cast<vectorized::IColumn*>(column.get()))
                    .insert(PyFloat_AsDouble(key_or_value_element));
        }
    } else if (which_type.is_float64()) {
        if (!PyFloat_Check(key_or_value_element)) {
            type_conflict = true;
        } else {
            reinterpret_cast<vectorized::ColumnNullable&>(
                    *const_cast<vectorized::IColumn*>(column.get()))
                    .insert(PyFloat_AsDouble(key_or_value_element));
        }
    } else if (which_type.is_string_or_fixed_string()) {
        if (!PyUnicode_Check(key_or_value_element)) {
            type_conflict = true;
        } else {
            const char* row_string = PyUnicode_AsUTF8(key_or_value_element);
            reinterpret_cast<vectorized::ColumnNullable&>(
                    *const_cast<vectorized::IColumn*>(column.get()))
                    .insert(std::string(row_string));
        }
    } else if (which_type.is_boolean()) {
        if (!PyBool_Check(key_or_value_element)) {
            type_conflict = true;
        } else {
            reinterpret_cast<vectorized::ColumnNullable&>(
                    *const_cast<vectorized::IColumn*>(column.get()))
                    .insert(Py_IsTrue(key_or_value_element));
        }
    } else {
        return Status::InvalidArgument(
                "Currently the defined python map key or value type is not supported : "
                "{}.",
                type->get_name());
    }
    if (type_conflict) {
        if (PyObject* py_type = PyObject_Type(key_or_value_element); py_type) {
            if (PyObject* py_type_str = PyObject_Str(py_type); py_type_str) {
                if (const char* type_name = PyUnicode_AsUTF8(py_type_str); type_name) {
                    return Status::InvalidArgument(
                            "Error: Python UDF returned an map with a key or value type "
                            "mismatch. "
                            "Expected key or value type {}, but found key or value type {}",
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

Status MapConverter::flatten_to_column_data(PyObject* py_object,
                                            const vectorized::DataTypePtr& type,
                                            vectorized::ColumnPtr& column) const {
    if (!PyList_Check(py_object)) {
        Py_CLEAR(py_object);
        return Status::InvalidArgument(
                "The function out type is ARRAY<MAP> but the actual "
                "python out type isn't Python List<Dict>.");
    }
    const size_t list_size = PyList_Size(py_object);
    for (int element_index = 0; element_index < list_size; ++element_index) {
        PyObject* py_list_element = PyList_GetItem(py_object, element_index);
        if (!PyDict_Check(py_list_element)) {
            return Status::InvalidArgument(
                    "The function out type is ARRAY<MAP> but the actual "
                    "python out list element isn't Python Dict.");
        }
        const auto& map_type = static_cast<const vectorized::DataTypeMap&>(*type);
        // 1. Get key type.
        const vectorized::DataTypePtr key_type = map_type.get_key_type();
        vectorized::DataTypePtr nested_key_type = key_type;
        if (nested_key_type->is_nullable()) {
            nested_key_type =
                    assert_cast<const vectorized::DataTypeNullable*>(nested_key_type.get())
                            ->get_nested_type();
        }
        // 2. Get value type.
        const vectorized::DataTypePtr value_type = map_type.get_value_type();
        vectorized::DataTypePtr nested_value_type = value_type;
        if (nested_value_type->is_nullable()) {
            nested_value_type =
                    assert_cast<const vectorized::DataTypeNullable*>(nested_value_type.get())
                            ->get_nested_type();
        }
        // 3. Create temporary columns for keys and values.
        vectorized::ColumnPtr nested_temp_keys_column;
        vectorized::ColumnPtr nested_temp_values_column;
        RETURN_IF_ERROR(
                generate_key_or_value_data_column(nested_key_type, nested_temp_keys_column));
        RETURN_IF_ERROR(
                generate_key_or_value_data_column(nested_value_type, nested_temp_values_column));
        vectorized::ColumnPtr temp_keyes_column = vectorized::ColumnNullable::create(
                nested_temp_keys_column,
                vectorized::ColumnUInt8::create(nested_temp_keys_column->size(), 0));
        vectorized::ColumnPtr temp_values_column = vectorized::ColumnNullable::create(
                nested_temp_values_column,
                vectorized::ColumnUInt8::create(nested_temp_values_column->size(), 0));
        PyObject *key, *value;
        Py_ssize_t pos = 0;
        while (PyDict_Next(py_list_element, &pos, &key, &value)) {
            RETURN_IF_ERROR(add_to_key_or_value_column(nested_key_type, temp_keyes_column, key));
            RETURN_IF_ERROR(
                    add_to_key_or_value_column(nested_value_type, temp_values_column, value));
        }
        // 4. Insert the temporary columns into the target column.
        vectorized::Array key_array_field;
        vectorized::Array value_array_field;
        for (size_t i = 0; i < temp_keyes_column->size(); ++i) {
            vectorized::Field key_field = (*temp_keyes_column)[i];
            vectorized::Field value_field = (*temp_values_column)[i];
            key_array_field.push_back(key_field);
            value_array_field.push_back(value_field);
        }
        reinterpret_cast<vectorized::ColumnMap&>(*const_cast<vectorized::IColumn*>(column.get()))
                .insert(vectorized::Map {key_array_field, value_array_field});
    }
    return Status::OK();
}

Status MapConverter::replicate_value_to_result_column(vectorized::ColumnPtr& source_column,
                                                      vectorized::MutableColumnPtr& result_column,
                                                      size_t index, size_t repeat_time) const {
    const auto map_data_field = reinterpret_cast<vectorized::ColumnMap&>(
            *const_cast<vectorized::IColumn*>(source_column.get()))[index];
    for (int i = 0; i < repeat_time; ++i) {
        reinterpret_cast<vectorized::ColumnMap&>(*(result_column.get())).insert(map_data_field);
    }
    return Status::OK();
}

Status MapConverter::copy_range_to_result_column(vectorized::ColumnPtr& source_column,
                                                 vectorized::MutableColumnPtr& result_column,
                                                 size_t start_index, size_t range_size) const {
    auto& target = reinterpret_cast<vectorized::ColumnMap&>(*result_column.get());
    int inc = 0;
    while (inc < range_size) {
        const auto map_data_field = reinterpret_cast<vectorized::ColumnMap&>(
                *const_cast<vectorized::IColumn*>(source_column.get()))[start_index + inc];
        target.insert(map_data_field);
        inc++;
    }
    return Status::OK();
}

} // namespace doris::pyudf