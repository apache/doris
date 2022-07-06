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

#include "vec/sink/vtablet_sink.h"

#include "util/brpc_client_cache.h"
#include "util/debug/sanitizer_scopes.h"
#include "util/doris_metrics.h"
#include "vec/core/block.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {
namespace stream_load {

VOlapTableSink::VOlapTableSink(ObjectPool* pool, const RowDescriptor& row_desc,
                               const std::vector<TExpr>& texprs, Status* status)
        : OlapTableSink(pool, row_desc, texprs, status) {
    // From the thrift expressions create the real exprs.
    vectorized::VExpr::create_expr_trees(pool, texprs, &_output_vexpr_ctxs);
    // Do not use the origin data scala expr, clear scala expr contexts
    _output_expr_ctxs.clear();
    _name = "VOlapTableSink";
}

Status VOlapTableSink::init(const TDataSink& sink) {
    RETURN_IF_ERROR(OlapTableSink::init(sink));
    _vpartition = _pool->add(new VOlapTablePartitionParam(_schema, sink.olap_table_sink.partition));
    return _vpartition->init();
}

Status VOlapTableSink::prepare(RuntimeState* state) {
    // Prepare the exprs to run.
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_output_vexpr_ctxs, state, _input_row_desc,
                                               _expr_mem_tracker));
    return OlapTableSink::prepare(state);
}

Status VOlapTableSink::open(RuntimeState* state) {
    // Prepare the exprs to run.
    RETURN_IF_ERROR(vectorized::VExpr::open(_output_vexpr_ctxs, state));
    return OlapTableSink::open(state);
}

size_t VOlapTableSink::get_pending_bytes() const {
    size_t mem_consumption = 0;
    for (auto& indexChannel : _channels) {
        mem_consumption += indexChannel->get_pending_bytes();
    }
    return mem_consumption;
}
Status VOlapTableSink::send(RuntimeState* state, vectorized::Block* input_block) {
    Status status = Status::OK();

    auto rows = input_block->rows();
    auto bytes = input_block->bytes();
    if (UNLIKELY(rows == 0)) {
        return status;
    }

    SCOPED_TIMER(_profile->total_time_counter());
    _number_input_rows += rows;
    // update incrementally so that FE can get the progress.
    // the real 'num_rows_load_total' will be set when sink being closed.
    state->update_num_rows_load_total(rows);
    state->update_num_bytes_load_total(bytes);
    DorisMetrics::instance()->load_rows->increment(rows);
    DorisMetrics::instance()->load_bytes->increment(bytes);

    vectorized::Block block(input_block->get_columns_with_type_and_name());
    if (!_output_vexpr_ctxs.empty()) {
        // Do vectorized expr here to speed up load
        block = vectorized::VExprContext::get_output_block_after_execute_exprs(
                _output_vexpr_ctxs, *input_block, status);
        if (UNLIKELY(block.rows() == 0)) {
            return status;
        }
    }

    auto num_rows = block.rows();
    int filtered_rows = 0;
    {
        SCOPED_RAW_TIMER(&_validate_data_ns);
        _filter_bitmap.Reset(block.rows());
        bool stop_processing = false;
        RETURN_IF_ERROR(
                _validate_data(state, &block, &_filter_bitmap, &filtered_rows, &stop_processing));
        _number_filtered_rows += filtered_rows;
        if (stop_processing) {
            // should be returned after updating "_number_filtered_rows", to make sure that load job can be cancelled
            // because of "data unqualified"
            return Status::EndOfFile("Encountered unqualified data, stop processing");
        }
        _convert_to_dest_desc_block(&block);
    }

    BlockRow block_row;
    SCOPED_RAW_TIMER(&_send_data_ns);
    // This is just for passing compilation.
    bool stop_processing = false;
    if (findTabletMode == FindTabletMode::FIND_TABLET_EVERY_BATCH) {
        _partition_to_tablet_map.clear();
    }

    size_t MAX_PENDING_BYTES = _load_mem_limit / 3;
    while (get_pending_bytes() > MAX_PENDING_BYTES && !state->is_cancelled()) {
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }

    for (int i = 0; i < num_rows; ++i) {
        if (filtered_rows > 0 && _filter_bitmap.Get(i)) {
            continue;
        }
        const VOlapTablePartition* partition = nullptr;
        uint32_t tablet_index = 0;
        block_row = {&block, i};
        if (!_vpartition->find_partition(&block_row, &partition)) {
            RETURN_IF_ERROR(state->append_error_msg_to_file(
                    []() -> std::string { return ""; },
                    [&]() -> std::string {
                        fmt::memory_buffer buf;
                        fmt::format_to(buf, "no partition for this tuple. tuple=[]");
                        return fmt::to_string(buf);
                    },
                    &stop_processing));
            _number_filtered_rows++;
            if (stop_processing) {
                return Status::EndOfFile("Encountered unqualified data, stop processing");
            }
            continue;
        }
        _partition_ids.emplace(partition->id);
        if (findTabletMode != FindTabletMode::FIND_TABLET_EVERY_ROW) {
            if (_partition_to_tablet_map.find(partition->id) == _partition_to_tablet_map.end()) {
                tablet_index = _vpartition->find_tablet(&block_row, *partition);
                _partition_to_tablet_map.emplace(partition->id, tablet_index);
            } else {
                tablet_index = _partition_to_tablet_map[partition->id];
            }
        } else {
            tablet_index = _vpartition->find_tablet(&block_row, *partition);
        }
        for (int j = 0; j < partition->indexes.size(); ++j) {
            int64_t tablet_id = partition->indexes[j].tablets[tablet_index];
            _channels[j]->add_row(block_row, tablet_id);
            _number_output_rows++;
        }
    }

    // check intolerable failure
    for (auto index_channel : _channels) {
        RETURN_IF_ERROR(index_channel->check_intolerable_failure());
    }
    return Status::OK();
}

Status VOlapTableSink::close(RuntimeState* state, Status exec_status) {
    if (_closed) return _close_status;
    vectorized::VExpr::close(_output_vexpr_ctxs, state);
    return OlapTableSink::close(state, exec_status);
}

Status VOlapTableSink::_validate_data(RuntimeState* state, vectorized::Block* block,
                                      Bitmap* filter_bitmap, int* filtered_rows,
                                      bool* stop_processing) {
    const auto num_rows = block->rows();
    fmt::memory_buffer error_msg;
    auto set_invalid_and_append_error_msg = [&](int row) {
        filter_bitmap->Set(row, true);
        return state->append_error_msg_to_file(
                []() -> std::string { return ""; },
                [&error_msg]() -> std::string { return fmt::to_string(error_msg); },
                stop_processing);
    };

    for (int i = 0; i < _output_tuple_desc->slots().size(); ++i) {
        SlotDescriptor* desc = _output_tuple_desc->slots()[i];
        block->get_by_position(i).column =
                block->get_by_position(i).column->convert_to_full_column_if_const();
        const auto& column = block->get_by_position(i).column;

        auto column_ptr = vectorized::check_and_get_column<vectorized::ColumnNullable>(*column);
        auto& real_column_ptr =
                column_ptr == nullptr ? column : (column_ptr->get_nested_column_ptr());

        switch (desc->type().type) {
        case TYPE_CHAR:
        case TYPE_VARCHAR:
        case TYPE_STRING: {
            const auto column_string =
                    assert_cast<const vectorized::ColumnString*>(real_column_ptr.get());

            size_t limit = std::min(config::string_type_length_soft_limit_bytes, desc->type().len);
            for (int j = 0; j < num_rows; ++j) {
                if (!filter_bitmap->Get(j)) {
                    auto str_val = column_string->get_data_at(j);
                    bool invalid = str_val.size > limit;
                    if (invalid) {
                        error_msg.clear();
                        if (str_val.size > desc->type().len) {
                            fmt::format_to(error_msg, "{}",
                                           "the length of input is too long than schema. ");
                            fmt::format_to(error_msg, "column_name: {}; ", desc->col_name());
                            fmt::format_to(error_msg, "input str: [{}] ", str_val.to_prefix(10));
                            fmt::format_to(error_msg, "schema length: {}; ", desc->type().len);
                            fmt::format_to(error_msg, "actual length: {}; ", str_val.size);
                        } else if (str_val.size > limit) {
                            fmt::format_to(
                                    error_msg, "{}",
                                    "the length of input string is too long than vec schema. ");
                            fmt::format_to(error_msg, "column_name: {}; ", desc->col_name());
                            fmt::format_to(error_msg, "input str: [{}] ", str_val.to_prefix(10));
                            fmt::format_to(error_msg, "schema length: {}; ", desc->type().len);
                            fmt::format_to(error_msg, "limit length: {}; ", limit);
                            fmt::format_to(error_msg, "actual length: {}; ", str_val.size);
                        }
                        RETURN_IF_ERROR(set_invalid_and_append_error_msg(j));
                    }
                }
            }
            break;
        }
        case TYPE_DECIMALV2: {
            auto column_decimal = const_cast<vectorized::ColumnDecimal<vectorized::Decimal128>*>(
                    assert_cast<const vectorized::ColumnDecimal<vectorized::Decimal128>*>(
                            real_column_ptr.get()));

            for (int j = 0; j < num_rows; ++j) {
                if (!filter_bitmap->Get(j)) {
                    auto dec_val = binary_cast<vectorized::Int128, DecimalV2Value>(
                            column_decimal->get_data()[j]);
                    error_msg.clear();
                    bool invalid = false;

                    if (dec_val.greater_than_scale(desc->type().scale)) {
                        auto code = dec_val.round(&dec_val, desc->type().scale, HALF_UP);
                        column_decimal->get_data()[j] =
                                binary_cast<DecimalV2Value, vectorized::Int128>(dec_val);

                        if (code != E_DEC_OK) {
                            fmt::format_to(error_msg, "round one decimal failed.value={}; ",
                                           dec_val.to_string());
                            invalid = true;
                        }
                    }
                    if (dec_val > _max_decimalv2_val[i] || dec_val < _min_decimalv2_val[i]) {
                        fmt::format_to(error_msg,
                                       "decimal value is not valid for definition, column={}",
                                       desc->col_name());
                        fmt::format_to(error_msg, ", value={}", dec_val.to_string());
                        fmt::format_to(error_msg, ", precision={}, scale={}; ",
                                       desc->type().precision, desc->type().scale);
                        invalid = true;
                    }

                    if (invalid) {
                        RETURN_IF_ERROR(set_invalid_and_append_error_msg(j));
                    }
                }
            }
            break;
        }
        default:
            break;
        }

        // Dispose the the column should do not contain the NULL value
        // Only tow case:
        // 1. column is nullable but the desc is not nullable
        // 2. desc->type is BITMAP
        if ((!desc->is_nullable() || desc->type() == TYPE_OBJECT) && column_ptr) {
            const auto& null_map = column_ptr->get_null_map_data();
            for (int j = 0; j < null_map.size(); ++j) {
                if (null_map[j] && !filter_bitmap->Get(j)) {
                    error_msg.clear();
                    fmt::format_to(error_msg, "null value for not null column, column={}; ",
                                   desc->col_name());
                    RETURN_IF_ERROR(set_invalid_and_append_error_msg(j));
                }
            }
        }
    }

    *filtered_rows = 0;
    for (int i = 0; i < num_rows; ++i) {
        *filtered_rows += filter_bitmap->Get(i);
    }
    return Status::OK();
}

void VOlapTableSink::_convert_to_dest_desc_block(doris::vectorized::Block* block) {
    for (int i = 0; i < _output_tuple_desc->slots().size(); ++i) {
        SlotDescriptor* desc = _output_tuple_desc->slots()[i];
        if (desc->is_nullable() != block->get_by_position(i).type->is_nullable()) {
            if (desc->is_nullable()) {
                block->get_by_position(i).type =
                        vectorized::make_nullable(block->get_by_position(i).type);
                block->get_by_position(i).column =
                        vectorized::make_nullable(block->get_by_position(i).column);
            } else {
                block->get_by_position(i).type = assert_cast<const vectorized::DataTypeNullable&>(
                                                         *block->get_by_position(i).type)
                                                         .get_nested_type();
                block->get_by_position(i).column = assert_cast<const vectorized::ColumnNullable&>(
                                                           *block->get_by_position(i).column)
                                                           .get_nested_column_ptr();
            }
        }
    }
}

} // namespace stream_load
} // namespace doris
