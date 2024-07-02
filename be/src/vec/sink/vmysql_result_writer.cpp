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

#include "vec/sink/vmysql_result_writer.h"

#include <fmt/core.h>
#include <gen_cpp/Data_types.h>
#include <gen_cpp/Metrics_types.h>
#include <glog/logging.h>
#include <stdint.h>
#include <string.h>
#include <sys/types.h>

#include <ostream>
#include <string>
#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "gutil/integral_types.h"
#include "olap/hll.h"
#include "runtime/buffer_control_block.h"
#include "runtime/decimalv2_value.h"
#include "runtime/define_primitive_type.h"
#include "runtime/large_int_value.h"
#include "runtime/primitive_type.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "util/binary_cast.hpp"
#include "util/jsonb_utils.h"
#include "util/quantile_state.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_struct.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/pod_array.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {
namespace vectorized {

template <bool is_binary_format>
VMysqlResultWriter<is_binary_format>::VMysqlResultWriter(BufferControlBlock* sinker,
                                                         const VExprContextSPtrs& output_vexpr_ctxs,
                                                         RuntimeProfile* parent_profile)
        : ResultWriter(),
          _sinker(sinker),
          _output_vexpr_ctxs(output_vexpr_ctxs),
          _parent_profile(parent_profile) {}

template <bool is_binary_format>
Status VMysqlResultWriter<is_binary_format>::init(RuntimeState* state) {
    _init_profile();
    // TODO: for PointQueryExecutor, the sinker is null, but we still need call init(),
    // so comment out this check temporarily.
    // if (nullptr == _sinker) {
    //     return Status::InternalError("sinker is NULL pointer.");
    // }
    set_output_object_data(state->return_object_data_as_binary());
    _is_dry_run = state->query_options().dry_run_query;

    RETURN_IF_ERROR(_set_options(state->query_options().serde_dialect));
    return Status::OK();
}

template <bool is_binary_format>
void VMysqlResultWriter<is_binary_format>::_init_profile() {
    if (_parent_profile != nullptr) {
        // for PointQueryExecutor, _parent_profile is null
        _append_row_batch_timer = ADD_TIMER(_parent_profile, "AppendBatchTime");
        _convert_tuple_timer =
                ADD_CHILD_TIMER(_parent_profile, "TupleConvertTime", "AppendBatchTime");
        _result_send_timer = ADD_CHILD_TIMER(_parent_profile, "ResultSendTime", "AppendBatchTime");
        _copy_buffer_timer = ADD_CHILD_TIMER(_parent_profile, "CopyBufferTime", "AppendBatchTime");
        _sent_rows_counter = ADD_COUNTER(_parent_profile, "NumSentRows", TUnit::UNIT);
        _bytes_sent_counter = ADD_COUNTER(_parent_profile, "BytesSent", TUnit::BYTES);
    }
}

template <bool is_binary_format>
Status VMysqlResultWriter<is_binary_format>::_set_options(
        const TSerdeDialect::type& serde_dialect) {
    switch (serde_dialect) {
    case TSerdeDialect::DORIS:
        // eg:
        //  array: ["abc", "def", "", null]
        //  map: {"k1":null, "k2":"v3"}
        _options.nested_string_wrapper = "\"";
        _options.wrapper_len = 1;
        _options.map_key_delim = ':';
        _options.null_format = "null";
        _options.null_len = 4;
        break;
    case TSerdeDialect::PRESTO:
        // eg:
        //  array: [abc, def, , NULL]
        //  map: {k1=NULL, k2=v3}
        _options.nested_string_wrapper = "";
        _options.wrapper_len = 0;
        _options.map_key_delim = '=';
        _options.null_format = "NULL";
        _options.null_len = 4;
        break;
    default:
        return Status::InternalError("unknown serde dialect: {}", serde_dialect);
    }
    return Status::OK();
}

template <bool is_binary_format>
Status VMysqlResultWriter<is_binary_format>::write(RuntimeState* state, Block& input_block) {
    SCOPED_TIMER(_append_row_batch_timer);
    Status status = Status::OK();
    if (UNLIKELY(input_block.rows() == 0)) {
        return status;
    }

    DCHECK(_output_vexpr_ctxs.empty() != true);

    // Exec vectorized expr here to speed up, block.rows() == 0 means expr exec
    // failed, just return the error status
    Block block;
    RETURN_IF_ERROR(VExprContext::get_output_block_after_execute_exprs(_output_vexpr_ctxs,
                                                                       input_block, &block));
    // convert one batch
    auto result = std::make_unique<TFetchDataResult>();
    auto num_rows = block.rows();
    result->result_batch.rows.resize(num_rows);
    uint64_t bytes_sent = 0;
    {
        SCOPED_TIMER(_convert_tuple_timer);
        MysqlRowBuffer<is_binary_format> row_buffer;
        if constexpr (is_binary_format) {
            row_buffer.start_binary_row(_output_vexpr_ctxs.size());
        }

        struct Arguments {
            const IColumn* column;
            bool is_const;
            DataTypeSerDeSPtr serde;
        };

        const size_t num_cols = _output_vexpr_ctxs.size();
        std::vector<Arguments> arguments;
        arguments.reserve(num_cols);

        for (size_t col_idx = 0; col_idx < num_cols; ++col_idx) {
            const auto& [column_ptr, col_const] =
                    unpack_if_const(block.get_by_position(col_idx).column);
            int scale = _output_vexpr_ctxs[col_idx]->root()->type().scale;
            // decimalv2 scale and precision is hard code, so we should get real scale and precision
            // from expr
            DataTypeSerDeSPtr serde;
            if (_output_vexpr_ctxs[col_idx]->root()->type().is_decimal_v2_type()) {
                if (_output_vexpr_ctxs[col_idx]->root()->is_nullable()) {
                    auto nested_serde =
                            std::make_shared<DataTypeDecimalSerDe<vectorized::Decimal128V2>>(scale,
                                                                                             27);
                    serde = std::make_shared<DataTypeNullableSerDe>(nested_serde);
                } else {
                    serde = std::make_shared<DataTypeDecimalSerDe<vectorized::Decimal128V2>>(scale,
                                                                                             27);
                }
            } else {
                serde = block.get_by_position(col_idx).type->get_serde();
            }
            serde->set_return_object_as_string(output_object_data());
            arguments.emplace_back(column_ptr.get(), col_const, serde);
        }

        for (size_t col_idx = 0; col_idx < num_cols; ++col_idx) {
            const auto& argument = arguments[col_idx];
            // const column will only have 1 row, see unpack_if_const
            if (argument.column->size() < num_rows && !argument.is_const) {
                return Status::InternalError(
                        "Required row size is out of range, need {} rows, column {} has {} "
                        "rows in fact.",
                        num_rows, argument.column->get_name(), argument.column->size());
            }
        }

        for (size_t row_idx = 0; row_idx < num_rows; ++row_idx) {
            for (size_t col_idx = 0; col_idx < num_cols; ++col_idx) {
                RETURN_IF_ERROR(arguments[col_idx].serde->write_column_to_mysql(
                        *(arguments[col_idx].column), row_buffer, row_idx,
                        arguments[col_idx].is_const, _options));
            }

            // copy MysqlRowBuffer to Thrift
            result->result_batch.rows[row_idx].append(row_buffer.buf(), row_buffer.length());
            bytes_sent += row_buffer.length();
            row_buffer.reset();
            if constexpr (is_binary_format) {
                row_buffer.start_binary_row(_output_vexpr_ctxs.size());
            }
        }
    }
    {
        SCOPED_TIMER(_result_send_timer);
        // If this is a dry run task, no need to send data block
        if (!_is_dry_run) {
            if (_sinker) {
                status = _sinker->add_batch(state, result);
            } else {
                _results.push_back(std::move(result));
            }
        }
        if (status.ok()) {
            _written_rows += num_rows;
            if (!_is_dry_run) {
                _bytes_sent += bytes_sent;
            }
        } else {
            LOG(WARNING) << "append result batch to sink failed.";
        }
    }
    return status;
}

template <bool is_binary_format>
Status VMysqlResultWriter<is_binary_format>::close(Status) {
    COUNTER_SET(_sent_rows_counter, _written_rows);
    COUNTER_UPDATE(_bytes_sent_counter, _bytes_sent);
    return Status::OK();
}

template class VMysqlResultWriter<true>;
template class VMysqlResultWriter<false>;

} // namespace vectorized
} // namespace doris
