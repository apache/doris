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

#include <algorithm>
#include <cstddef>
#include <vec/data_types/data_type_factory.hpp>

#include "common/status.h"
#include "data_types/data_type_converter.h"
#include "engine/python_udtf_executor.h"
#include "vec/columns/column_nullable.h"
#include "vec/data_types/data_type.h"
#include "vec/exprs/table_function/table_function.h"

namespace doris::vectorized {

/**
 * The {@link PythonUDTFunction} class represents a Python UDTF (User Defined Table Function).
 * It extends the {@link TableFunction} interface to provide functionality for executing Python UDTFs
 * within the vectorized SQL engine. This class handles the setup, execution, and teardown of Python UDTFs,
 * ensuring that the UDTFs are executed in a separate subprocess to avoid GIL (Global Interpreter Lock) issues.
 *
 * The class is responsible for:
 * - Creating and managing the Python UDTF environment.
 * - Preparing and executing the UDTF on input columns.
 * - Handling the lifecycle of the UDTF execution, including setup and cleanup.
 * - Managing the result cache to optimize performance when multiple UDTFs are used in a single query.
 */
class PythonUDTFunction final : public TableFunction {
    ENABLE_FACTORY_CREATOR(PythonUDTFunction);

    explicit PythonUDTFunction(const TFunction& t_fn) : TableFunction(), _t_fn(t_fn) {
        _fn_name = _t_fn.name.function_name;
        _memory_cache_type = DataTypeFactory::instance().create_data_type(
                TypeDescriptor::from_thrift(t_fn.ret_type));
        if (_memory_cache_type->is_nullable()) {
            _memory_cache_type = remove_nullable(_memory_cache_type);
        }
    }

    ~PythonUDTFunction() override = default;

    /**
     * Open the udtf.
     */
    Status open() override;

    /**
     * Initialize the udtf by one block.
     *
     * @param block the input data block.
     * @param state the runtime state.
     * @return status.
     */
    Status process_init(Block* block, RuntimeState* state) override;

    /**
     * Invoke the udtf by one row.
     *
     * @param row_idx the row index.
     */
    void process_row(size_t row_idx) override;

    void process_close() override;

    /**
     * For a given row of input data, get the result of the udtf.
     *
     * @param column the result column.
     * @param max_step the max udtf result size that should be returned.
     * @return status.
     */
    int get_value(MutableColumnPtr& column, int max_step) override;

    /**
     * If there are multiple udtfs in one SELECT statement, this
     * method directly copies the result to avoid repeated calls to
     * get_value.
     *
     * @param column the result column.
     * @param repeat_time the repeat times of data copy.
     */
    void get_same_many_values(MutableColumnPtr& column, int repeat_time) override;

    Status close() override;

private:
    const TFunction& _t_fn;
    std::vector<ColumnPtr> _udf_input_columns;
    int _udf_input_column_number;
    DataTypes _input_column_types;
    ColumnPtr _memory_cache;
    DataTypePtr _memory_cache_type;
    std::shared_ptr<pyudf::DataTypeConverter> _memory_cache_converter;
    pyudf::PythonUdtfExecutor executor;

    /**
     * Update the next batch result.
     */
    void update_next_batch();
};

} // namespace doris::vectorized
