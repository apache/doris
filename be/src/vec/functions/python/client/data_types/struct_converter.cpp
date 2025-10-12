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

#include "struct_converter.h"

#include <vec/columns/column_struct.h>
#include <vec/data_types/data_type_nullable.h>
#include <vec/data_types/data_type_struct.h>

#include "data_type_converter_factory.h"

doris::Status doris::pyudf::StructConverter::create_column(const vectorized::DataTypePtr& type,
                                                           vectorized::ColumnPtr& column) const {
    const auto& struct_type = static_cast<const vectorized::DataTypeStruct&>(*type);
    // 1. Create the column of each struct element.
    const vectorized::DataTypes element_types = struct_type.get_elements();
    std::vector<vectorized::ColumnPtr> element_columns;
    for (const auto& elem_type : element_types) {
        vectorized::ColumnPtr elem_column;
        auto _elem_type = elem_type;
        if (elem_type->is_nullable()) {
            _elem_type = remove_nullable(elem_type);
        }
        const auto column_converter = DataTypeConverterFactory::create_converter(_elem_type);
        if (!column_converter) {
            return Status::InvalidArgument("Unsupported struct element type: {}.",
                                           elem_type->get_name());
        }
        RETURN_IF_ERROR(column_converter->create_column(_elem_type, elem_column));
        element_columns.emplace_back(std::move(elem_column));
    }
    // 2. Create the struct column.
    column = vectorized::ColumnStruct::create(element_columns);
    return Status::OK();
}

doris::Status doris::pyudf::StructConverter::flatten_to_column_data(
        PyObject* py_object, const vectorized::DataTypePtr& type,
        vectorized::ColumnPtr& column) const {
    if (!PyList_Check(py_object)) {
        Py_CLEAR(py_object);
        return Status::InvalidArgument("Expected Python list for ARRAY<STRUCT> output type.");
    }
    const auto& struct_type = static_cast<const vectorized::DataTypeStruct&>(*type);
    const int column_size = struct_type.get_elements().size();
    for (int index = 0; index < column_size; ++index) {
        PyObject* column_object = PyList_New(0);
        const size_t list_size = PyList_Size(py_object);
        for (size_t list_index = 0; list_index < list_size; ++list_index) {
            PyObject* py_list_element = PyList_GetItem(py_object, list_index);
            if (!PyTuple_Check(py_list_element)) {
                return Status::InvalidArgument(
                        "The function out type is ARRAY<STRUCT> but the actual "
                        "python out list element isn't Python Tuple.");
            }
            if (const int actual_result_column_size = PyTuple_Size(py_list_element);
                actual_result_column_size != column_size) {
                return Status::InvalidArgument(
                        "The python udtf function is expected to output {} results"
                        ", but there're {} results.",
                        struct_type.get_elements().size(), actual_result_column_size);
            }
            if (PyObject* py_tuple_element = PyTuple_GetItem(py_list_element, index);
                PyList_Append(column_object, py_tuple_element) < 0) {
                return Status::InternalError("Failed to append batch result to output list.");
            }
        }
        // 1. Get the actual data type of each column.
        vectorized::DataTypePtr element_column_type = struct_type.get_element(index);
        if (element_column_type->is_nullable()) {
            element_column_type =
                    assert_cast<const vectorized::DataTypeNullable*>(element_column_type.get())
                            ->get_nested_type();
        }
        // 2. Insert the python data to each column.
        auto element_column = reinterpret_cast<vectorized::ColumnStruct&>(
                                      *const_cast<vectorized::IColumn*>(column.get()))
                                      .get_column_ptr(index);
        const auto element_converter =
                DataTypeConverterFactory::create_converter(element_column_type);
        RETURN_IF_ERROR(element_converter->flatten_to_column_data(
                column_object, element_column_type, element_column));
    }
    return Status::OK();
}

doris::Status doris::pyudf::StructConverter::replicate_value_to_result_column(
        vectorized::ColumnPtr& source_column, vectorized::MutableColumnPtr& result_column,
        size_t index, size_t repeat_time) const {
    const auto struct_data_field = reinterpret_cast<vectorized::ColumnStruct&>(
            *const_cast<vectorized::IColumn*>(source_column.get()))[index];
    for (int i = 0; i < repeat_time; ++i) {
        reinterpret_cast<vectorized::ColumnArray&>(*(result_column.get()))
                .insert(struct_data_field);
    }
    return Status::OK();
}

doris::Status doris::pyudf::StructConverter::copy_range_to_result_column(
        vectorized::ColumnPtr& source_column, vectorized::MutableColumnPtr& result_column,
        size_t start_index, size_t range_size) const {
    auto& target = reinterpret_cast<vectorized::ColumnStruct&>(*result_column.get());
    int inc = 0;
    while (inc < range_size) {
        const auto struct_data_field = reinterpret_cast<vectorized::ColumnStruct&>(
                *const_cast<vectorized::IColumn*>(source_column.get()))[start_index + inc];
        target.insert(struct_data_field);
        inc++;
    }
    return Status::OK();
}
