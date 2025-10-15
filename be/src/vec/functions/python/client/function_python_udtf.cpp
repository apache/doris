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

#include "function_python_udtf.h"

#include <runtime/user_function_cache.h>

#include <ostream>
#include <vector>

#include "common/status.h"
#include "data_types/data_type_converter_factory.h"
#include "runtime/runtime_state.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/functions/python/client/engine/python_env.hpp"

namespace doris::vectorized {

Status PythonUDTFunction::open() {
    RETURN_IF_ERROR(pyudf::PythonEnv::init_env());
    std::string script_location;
    auto* function_cache = UserFunctionCache::instance();
    RETURN_IF_ERROR(function_cache->get_pypath(_t_fn.id, _t_fn.hdfs_location, _t_fn.checksum,
                                               &script_location));
    RETURN_IF_ERROR(executor.setup(script_location, _t_fn.scalar_fn.symbol, _t_fn.python_udf_null_on_failure));
    return Status::OK();
}

Status PythonUDTFunction::process_init(Block* block, RuntimeState* state) {
    const auto child_size = _expr_context->root()->get_num_children();
    std::vector<size_t> input_column_idxs;
    input_column_idxs.resize(child_size);
    for (int i = 0; i < child_size; ++i) {
        int result_id = -1;
        RETURN_IF_ERROR(_expr_context->root()->children()[i]->execute(_expr_context.get(), block,
                                                                      &result_id));
        auto argument_type = _expr_context->root()->children()[i]->data_type();
        if (argument_type->is_nullable()) {
            argument_type = remove_nullable(argument_type);
        }
        _input_column_types.push_back(argument_type);
        DCHECK_NE(result_id, -1);
        input_column_idxs[i] = result_id;
    }
    // 1. Create the input columns
    _udf_input_columns.clear();
    _udf_input_column_number = child_size;
    _udf_input_columns.resize(child_size);
    for (size_t i = 0; i < child_size; ++i) {
        ColumnWithTypeAndName& column = block->get_by_position(input_column_idxs[i]);
        const auto single_column = column.column->convert_to_full_column_if_const();
        _udf_input_columns[i] = single_column;
    }

    // 2. Create the output column
    _memory_cache = _memory_cache_type->create_column();
    _memory_cache_converter = pyudf::DataTypeConverterFactory::create_converter(_memory_cache_type);
    return Status::OK();
}

void PythonUDTFunction::process_row(size_t row_idx) {
    TableFunction::process_row(row_idx);
    THROW_IF_ERROR(executor.init(_udf_input_columns.data(), _udf_input_column_number, row_idx,
                                 _input_column_types, _memory_cache, _memory_cache_type));
    update_next_batch();
}

void PythonUDTFunction::process_close() {
    _udf_input_columns.clear();
    _memory_cache = nullptr;
}

void PythonUDTFunction::get_same_many_values(MutableColumnPtr& column, int repeat_time) {
    if (current_empty()) {
        column->insert_many_defaults(repeat_time);
    } else {
        if (_is_nullable) {
            MutableColumnPtr target_column =
                    reinterpret_cast<ColumnNullable&>(*column.get()).get_nested_column_ptr();
            assert_cast<ColumnUInt8*>(
                    assert_cast<ColumnNullable&>(*column.get()).get_null_map_column_ptr().get())
                    ->insert_many_defaults(repeat_time);
            THROW_IF_ERROR(_memory_cache_converter->replicate_value_to_result_column(
                    _memory_cache, target_column, _cur_offset, repeat_time));
        } else {
            THROW_IF_ERROR(_memory_cache_converter->replicate_value_to_result_column(
                    _memory_cache, column, _cur_offset, repeat_time));
        }
    }
    update_next_batch();
}

int PythonUDTFunction::get_value(MutableColumnPtr& column, int max_step) {
    max_step = std::min(max_step, static_cast<int>(_cur_size - _cur_offset));
    if (current_empty()) {
        column->insert_default();
        max_step = 1;
    } else {
        if (_is_nullable) {
            MutableColumnPtr target_column =
                    reinterpret_cast<ColumnNullable&>(*column.get()).get_nested_column_ptr();
            assert_cast<ColumnUInt8*>(
                    assert_cast<ColumnNullable&>(*column.get()).get_null_map_column_ptr().get())
                    ->insert_many_defaults(max_step);
            RETURN_IF_ERROR(_memory_cache_converter->copy_range_to_result_column(
                    _memory_cache, target_column, _cur_offset, max_step));
        } else {
            RETURN_IF_ERROR(_memory_cache_converter->copy_range_to_result_column(
                    _memory_cache, column, _cur_offset, max_step));
        }
    }
    update_next_batch();
    forward(max_step);
    return max_step;
}

Status PythonUDTFunction::close() {
    RETURN_IF_ERROR(executor.close());
    return TableFunction::close();
}

void PythonUDTFunction::update_next_batch() {
    THROW_IF_ERROR(executor.fetch_next_batch_result(_memory_cache, _memory_cache_type));
    _cur_size = std::max(0, static_cast<int>(_memory_cache->size()));
}

} // namespace doris::vectorized
