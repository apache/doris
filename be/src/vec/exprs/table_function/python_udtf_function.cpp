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

#include "vec/exprs/table_function/python_udtf_function.h"

#include <arrow/array.h>
#include <arrow/record_batch.h>
#include <arrow/type_fwd.h>
#include <glog/logging.h>

#include "runtime/runtime_state.h"
#include "runtime/user_function_cache.h"
#include "udf/python/python_env.h"
#include "udf/python/python_udf_meta.h"
#include "udf/python/python_udf_server.h"
#include "util/arrow/block_convertor.h"
#include "util/arrow/row_batch.h"
#include "util/arrow/utils.h"
#include "util/timezone_utils.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/functions/array/function_array_utils.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

PythonUDTFFunction::PythonUDTFFunction(const TFunction& t_fn) : TableFunction(), _t_fn(t_fn) {
    _fn_name = _t_fn.name.function_name;
    // Initialize timezone to UTC
    TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, _timezone_obj);

    // Like Java UDTF, FE passes the element type T, and we wrap it into array<T> here
    // This makes the behavior consistent with Java UDTF
    DataTypePtr element_type = DataTypeFactory::instance().create_data_type(t_fn.ret_type);
    _return_type = make_nullable(std::make_shared<DataTypeArray>(make_nullable(element_type)));
}

Status PythonUDTFFunction::open() {
    // Create Python UDF metadata
    PythonUDFMeta python_udf_meta;
    python_udf_meta.id = _t_fn.id;
    python_udf_meta.name = _t_fn.name.function_name;
    python_udf_meta.symbol = _t_fn.scalar_fn.symbol;

    if (!_t_fn.function_code.empty()) {
        python_udf_meta.type = PythonUDFLoadType::INLINE;
        python_udf_meta.location = "inline";
        python_udf_meta.inline_code = _t_fn.function_code;
    } else if (!_t_fn.hdfs_location.empty()) {
        python_udf_meta.type = PythonUDFLoadType::MODULE;
        python_udf_meta.location = _t_fn.hdfs_location;
        python_udf_meta.checksum = _t_fn.checksum;
    } else {
        python_udf_meta.type = PythonUDFLoadType::UNKNOWN;
        python_udf_meta.location = "unknown";
    }

    python_udf_meta.client_type = PythonClientType::UDTF;

    if (python_udf_meta.type == PythonUDFLoadType::MODULE) {
        RETURN_IF_ERROR(UserFunctionCache::instance()->get_pypath(
                python_udf_meta.id, python_udf_meta.location, python_udf_meta.checksum,
                &python_udf_meta.location));
    }

    PythonVersion version;
    if (_t_fn.__isset.runtime_version && !_t_fn.runtime_version.empty()) {
        RETURN_IF_ERROR(
                PythonVersionManager::instance().get_version(_t_fn.runtime_version, &version));
        python_udf_meta.runtime_version = version.full_version;
    } else {
        return Status::InvalidArgument("Python UDTF runtime version is not set");
    }

    for (const auto& arg_type : _t_fn.arg_types) {
        DataTypePtr doris_type = DataTypeFactory::instance().create_data_type(arg_type);
        python_udf_meta.input_types.push_back(doris_type);
    }

    // For Python UDTF, FE passes the element type T (like Java UDTF)
    // Use it directly as the UDF's return type for Python metadata
    python_udf_meta.return_type = DataTypeFactory::instance().create_data_type(_t_fn.ret_type);
    python_udf_meta.always_nullable = python_udf_meta.return_type->is_nullable();
    RETURN_IF_ERROR(python_udf_meta.check());

    RETURN_IF_ERROR(
            PythonUDFServerManager::instance().get_client(python_udf_meta, version, &_udtf_client));

    if (!_udtf_client) {
        return Status::InternalError("Failed to create Python UDTF client");
    }

    return Status::OK();
}

Status PythonUDTFFunction::process_init(Block* block, RuntimeState* state) {
    // Step 1: Extract input columns from child expressions
    auto child_size = _expr_context->root()->children().size();
    ColumnNumbers child_column_idxs;
    child_column_idxs.resize(child_size);
    for (int i = 0; i < child_size; ++i) {
        int result_id = -1;
        RETURN_IF_ERROR(_expr_context->root()->children()[i]->execute(_expr_context.get(), block,
                                                                      &result_id));
        DCHECK_NE(result_id, -1);
        child_column_idxs[i] = result_id;
    }

    // Step 2: Build input block and convert to Arrow format
    vectorized::Block input_block;
    for (uint32_t i = 0; i < child_column_idxs.size(); ++i) {
        input_block.insert(block->get_by_position(child_column_idxs[i]));
    }
    std::shared_ptr<arrow::Schema> input_schema;
    std::shared_ptr<arrow::RecordBatch> input_batch;
    RETURN_IF_ERROR(get_arrow_schema_from_block(input_block, &input_schema,
                                                TimezoneUtils::default_time_zone));
    RETURN_IF_ERROR(convert_to_arrow_batch(input_block, input_schema, arrow::default_memory_pool(),
                                           &input_batch, _timezone_obj));

    // Step 3: Call Python UDTF to evaluate all rows at once (similar to Java UDTF's JNI call)
    // Python returns offsets array + flattened data batch
    std::shared_ptr<arrow::Int64Array> offsets_array;
    std::shared_ptr<arrow::RecordBatch> data_batch;

    RETURN_IF_ERROR(_udtf_client->evaluate(*input_batch, &offsets_array, &data_batch));

    // Step 4: Create array result column
    _array_result_column = _return_type->create_column();

    // Step 5: Convert Python server output (offsets + data) to Doris array column
    RETURN_IF_ERROR(_convert_offsets_and_data_to_array_column(offsets_array, data_batch,
                                                              input_block.rows()));

    // Step 6: Extract array column metadata using extract_column_array_info
    if (!extract_column_array_info(*_array_result_column, _array_column_detail)) {
        return Status::NotSupported("column type {} not supported now",
                                    _array_result_column->get_name());
    }

    return Status::OK();
}

void PythonUDTFFunction::process_row(size_t row_idx) {
    TableFunction::process_row(row_idx);

    // Check if array is null for this row
    if (!_array_column_detail.array_nullmap_data ||
        !_array_column_detail.array_nullmap_data[row_idx]) {
        _array_offset = (*_array_column_detail.offsets_ptr)[row_idx - 1];
        _cur_size = (*_array_column_detail.offsets_ptr)[row_idx] - _array_offset;
    }
    // When it's NULL at row_idx, _cur_size stays 0, meaning current_empty()
    // If outer function: will continue with insert_default
    // If not outer function: will not insert any value
}

void PythonUDTFFunction::process_close() {
    _array_result_column = nullptr;
    _array_column_detail.reset();
    _array_offset = 0;
}

void PythonUDTFFunction::get_same_many_values(MutableColumnPtr& column, int length) {
    size_t pos = _array_offset + _cur_offset;
    if (current_empty() || (_array_column_detail.nested_nullmap_data &&
                            _array_column_detail.nested_nullmap_data[pos])) {
        column->insert_many_defaults(length);
    } else {
        if (_is_nullable) {
            auto* nullable_column = assert_cast<ColumnNullable*>(column.get());
            auto nested_column = nullable_column->get_nested_column_ptr();
            auto nullmap_column = nullable_column->get_null_map_column_ptr();
            nested_column->insert_many_from(*_array_column_detail.nested_col, pos, length);
            assert_cast<ColumnUInt8*>(nullmap_column.get())->insert_many_defaults(length);
        } else {
            column->insert_many_from(*_array_column_detail.nested_col, pos, length);
        }
    }
}

int PythonUDTFFunction::get_value(MutableColumnPtr& column, int max_step) {
    max_step = std::min(max_step, (int)(_cur_size - _cur_offset));
    size_t pos = _array_offset + _cur_offset;

    if (current_empty()) {
        column->insert_default();
        max_step = 1;
    } else {
        if (_is_nullable) {
            auto* nullable_column = assert_cast<ColumnNullable*>(column.get());
            auto nested_column = nullable_column->get_nested_column_ptr();
            auto* nullmap_column =
                    assert_cast<ColumnUInt8*>(nullable_column->get_null_map_column_ptr().get());

            nested_column->insert_range_from(*_array_column_detail.nested_col, pos, max_step);
            size_t old_size = nullmap_column->size();
            nullmap_column->resize(old_size + max_step);
            memcpy(nullmap_column->get_data().data() + old_size,
                   _array_column_detail.nested_nullmap_data + pos * sizeof(UInt8),
                   max_step * sizeof(UInt8));
        } else {
            column->insert_range_from(*_array_column_detail.nested_col, pos, max_step);
        }
    }
    forward(max_step);
    return max_step;
}

Status PythonUDTFFunction::close() {
    // Close UDTF client
    if (_udtf_client) {
        Status status = _udtf_client->close();
        if (!status.ok()) {
            LOG(WARNING) << "Failed to close UDTF client: " << status.to_string();
        }
        _udtf_client.reset();
    }

    return TableFunction::close();
}

Status PythonUDTFFunction::_convert_offsets_and_data_to_array_column(
        const std::shared_ptr<arrow::Int64Array>& offsets_array,
        const std::shared_ptr<arrow::RecordBatch>& data_batch, size_t num_input_rows) {
    if (offsets_array->length() != static_cast<int64_t>(num_input_rows + 1)) {
        return Status::InternalError(
                "Invalid offsets array size: expected {} elements for {} input rows, got {}",
                num_input_rows + 1, num_input_rows, offsets_array->length());
    }

    auto get_array_column_ptrs = [&](ColumnArray*& array_col, ColumnNullable*& nullable_col) {
        if (_return_type->is_nullable()) {
            nullable_col =
                    assert_cast<ColumnNullable*>(_array_result_column->assume_mutable().get());
            array_col = assert_cast<ColumnArray*>(
                    nullable_col->get_nested_column_ptr()->assume_mutable().get());
        } else {
            array_col = assert_cast<ColumnArray*>(_array_result_column->assume_mutable().get());
            nullable_col = nullptr;
        }
    };

    int64_t total_output_rows = offsets_array->Value(num_input_rows);

    // Handle case where all input rows produce zero output rows
    // This can happen in two ways:
    // 1. Python server returns data_batch = None (nullptr)
    // 2. Python server returns empty RecordBatch (0 rows)
    if (!data_batch || total_output_rows == 0) {
        // Verify that offsets indicate zero output rows
        if (total_output_rows != 0) {
            return Status::InternalError("Empty data_batch but offsets indicate {} output rows",
                                         total_output_rows);
        }

        // Build array column with all empty arrays
        ColumnArray* array_column = nullptr;
        ColumnNullable* nullable_col = nullptr;
        get_array_column_ptrs(array_column, nullable_col);

        auto& nested_offsets = array_column->get_offsets();

        // Insert empty arrays for all input rows
        for (size_t i = 0; i < num_input_rows; ++i) {
            // Mark array as non-null (empty array != NULL)
            if (nullable_col) {
                nullable_col->get_null_map_column().insert_default();
            }
            // Push offset (no data inserted, so offset stays same)
            nested_offsets.push_back(nested_offsets.empty() ? 0 : nested_offsets.back());
        }

        return Status::OK();
    }

    if (data_batch->num_columns() != 1) {
        return Status::InternalError("Invalid data batch: expected 1 column, got {}",
                                     data_batch->num_columns());
    }

    DataTypePtr element_type = DataTypeFactory::instance().create_data_type(_t_fn.ret_type);

    // Convert Arrow batch to Doris column using element_type directly
    DataTypes output_types = {make_nullable(element_type)};
    vectorized::Block output_block;
    RETURN_IF_ERROR(
            convert_from_arrow_batch(data_batch, output_types, &output_block, _timezone_obj));

    if (total_output_rows != static_cast<int64_t>(output_block.rows())) {
        return Status::InternalError(
                "Mismatched output rows: offsets indicate {} rows, but data batch has {} rows",
                total_output_rows, output_block.rows());
    }

    ColumnPtr output_column;
    if (output_block.columns() > 0) {
        // Use the converted column directly (already matches element_type)
        output_column = output_block.get_by_position(0).column;
    } else {
        return Status::InternalError("Output block has no columns");
    }

    // Get mutable array column pointers
    ColumnArray* array_column = nullptr;
    ColumnNullable* nullable_col = nullptr;
    get_array_column_ptrs(array_column, nullable_col);

    auto& nested_column = array_column->get_data();
    auto& nested_offsets = array_column->get_offsets();

    for (size_t i = 0; i < num_input_rows; ++i) {
        // Mark this array element as non-null (if nullable)
        if (nullable_col) {
            nullable_col->get_null_map_column().insert_default();
        }

        // Extract range for this input row from the flattened output
        int64_t start_offset = offsets_array->Value(i);
        int64_t end_offset = offsets_array->Value(i + 1);
        int64_t count = end_offset - start_offset;

        if (count > 0) {
            nested_column.insert_range_from(*output_column, start_offset, count);
        }

        nested_offsets.push_back(nested_column.size());
    }

    return Status::OK();
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized
