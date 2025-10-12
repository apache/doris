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
 * {@link StructConverter} is a specialized implementation of {@link DataTypeConverter}, designed
 * only for the User-Defined Table Function (UDTF) scenario where multiple columns need to be returned.
 *
 * The STRUCT data type represents a combination of multiple columns. Therefore, in UDTF, we map
 * the multiple columns of computed results into a single STRUCT type. The {@link StructConverter} is
 * responsible for performing this data conversion process.
 */
class StructConverter final : public DataTypeConverter {
public:
    Status flatten_to_column_data(PyObject* py_object, const vectorized::DataTypePtr& type,
                                  vectorized::ColumnPtr& column) const override;

    Status replicate_value_to_result_column(vectorized::ColumnPtr& source_column,
                                            vectorized::MutableColumnPtr& result_column,
                                            size_t index, size_t repeat_time) const override;

    Status create_column(const vectorized::DataTypePtr& type,
                         vectorized::ColumnPtr& column) const override;

    Status copy_range_to_result_column(vectorized::ColumnPtr& source_column,
                                       vectorized::MutableColumnPtr& result_column,
                                       size_t start_index, size_t range_size) const override;

    Status convert_to_python_object(const vectorized::ColumnPtr& column,
                                    const vectorized::DataTypePtr& type, size_t row_index,
                                    PyObject*& py_object) const override {
        return Status::NotSupported("STRUCT data type is not supported.");
    }

    Status convert_to_column_data(PyObject* py_object, const vectorized::DataTypePtr& type,
                                  vectorized::ColumnPtr& column) const override {
        return Status::NotSupported("STRUCT data type is not supported.");
    }

    Status insert_column_default_data(const vectorized::DataTypePtr& type,
                                      vectorized::ColumnPtr& column) const override {
        return Status::NotSupported("STRUCT data type is not supported.");
    }
};

} // namespace doris::pyudf