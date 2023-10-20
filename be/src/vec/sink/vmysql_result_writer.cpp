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

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
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
#include "util/bitmap_value.h"
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
    if (nullptr == _sinker) {
        return Status::InternalError("sinker is NULL pointer.");
    }
    set_output_object_data(state->return_object_data_as_binary());
    _is_dry_run = state->query_options().dry_run_query;
    _enable_faster_float_convert = state->enable_faster_float_convert();

    return Status::OK();
}

template <bool is_binary_format>
void VMysqlResultWriter<is_binary_format>::_init_profile() {
    _append_row_batch_timer = ADD_TIMER(_parent_profile, "AppendBatchTime");
    _convert_tuple_timer = ADD_CHILD_TIMER(_parent_profile, "TupleConvertTime", "AppendBatchTime");
    _result_send_timer = ADD_CHILD_TIMER(_parent_profile, "ResultSendTime", "AppendBatchTime");
    _copy_buffer_timer = ADD_CHILD_TIMER(_parent_profile, "CopyBufferTime", "AppendBatchTime");
    _sent_rows_counter = ADD_COUNTER(_parent_profile, "NumSentRows", TUnit::UNIT);
    _bytes_sent_counter = ADD_COUNTER(_parent_profile, "BytesSent", TUnit::BYTES);
}

template <bool is_binary_format>
Status VMysqlResultWriter<is_binary_format>::append_block(Block& input_block) {
    SCOPED_TIMER(_append_row_batch_timer);
    Status status = Status::OK();
    if (UNLIKELY(input_block.rows() == 0)) {
        return status;
    }

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
        row_buffer.set_faster_float_convert(_enable_faster_float_convert);
        if constexpr (is_binary_format) {
            row_buffer.start_binary_row(_output_vexpr_ctxs.size());
        }

        struct Arguments {
            const IColumn* column;
            bool is_const;
            DataTypeSerDeSPtr serde;
        };

        std::vector<Arguments> arguments;
        for (int i = 0; i < _output_vexpr_ctxs.size(); ++i) {
            const auto& [column_ptr, col_const] = unpack_if_const(block.get_by_position(i).column);
            int scale = _output_vexpr_ctxs[i]->root()->type().scale;
            // decimalv2 scale and precision is hard code, so we should get real scale and precision
            // from expr
            DataTypeSerDeSPtr serde;
            if (_output_vexpr_ctxs[i]->root()->type().is_decimal_v2_type()) {
                serde = std::make_shared<DataTypeDecimalSerDe<vectorized::Decimal128>>(scale, 27);
            } else {
                serde = block.get_by_position(i).type->get_serde();
            }
            serde->set_return_object_as_string(output_object_data());
            arguments.emplace_back(column_ptr.get(), col_const, serde);
        }

        for (size_t row_idx = 0; row_idx != num_rows; ++row_idx) {
            for (int i = 0; i < _output_vexpr_ctxs.size(); ++i) {
                RETURN_IF_ERROR(arguments[i].serde->write_column_to_mysql(
                        *(arguments[i].column), row_buffer, row_idx, arguments[i].is_const));
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
                status = _sinker->add_batch(result);
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
bool VMysqlResultWriter<is_binary_format>::can_sink() {
    return _sinker->can_sink();
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
