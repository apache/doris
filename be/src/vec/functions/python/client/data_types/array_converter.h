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
#include <common/status.h>
#include <vec/data_types/data_type.h>

#include "data_type_converter.h"

namespace doris::pyudf {

/**
 * {@link ArrayConverter} is the specific implementation of {@link DataTypeConverter}, which is
 * responsible for converting between ARRAY<T> column data and Python objects.
 */
class ArrayConverter final : public DataTypeConverter {
public:
    Status convert_to_python_object(const vectorized::ColumnPtr& column,
                                    const vectorized::DataTypePtr& type, size_t row_index,
                                    PyObject*& py_object) const override;

    Status convert_to_column_data(PyObject* py_object, const vectorized::DataTypePtr& type,
                                  vectorized::ColumnPtr& column) const override;

    Status create_column(const vectorized::DataTypePtr& type,
                         vectorized::ColumnPtr& column) const override;

    Status insert_column_default_data(const vectorized::DataTypePtr& type,
                                      vectorized::ColumnPtr& column) const override;

    Status flatten_to_column_data(PyObject* py_object, const vectorized::DataTypePtr& type,
                                  vectorized::ColumnPtr& column) const override;

    Status replicate_value_to_result_column(vectorized::ColumnPtr& source_column,
                                            vectorized::MutableColumnPtr& result_column,
                                            size_t index, size_t repeat_time) const override;

    Status copy_range_to_result_column(vectorized::ColumnPtr& source_column,
                                       vectorized::MutableColumnPtr& result_column,
                                       size_t start_index, size_t range_size) const override;

private:
    static Status generate_array_data_column(const vectorized::DataTypePtr& type,
                                             vectorized::ColumnPtr& column);

    static Status add_to_array_data_column(vectorized::DataTypePtr& type,
                                           vectorized::ColumnPtr& column, PyObject* py_element);
};

} // namespace doris::pyudf