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

#include "vec/exec/vbroker_scanner.h"

#include <fmt/format.h>
#include <iostream>
#include <sstream>

#include "exec/text_converter.h"
#include "exec/exec_node.h"
#include "exprs/expr_context.h"
#include "exec/plain_text_line_reader.h"
#include "util/utf8_check.h"

namespace doris::vectorized {

bool is_null(const Slice& slice) {
    return slice.size == 2 && slice.data[0] == '\\' && slice.data[1] == 'N';
}

VBrokerScanner::VBrokerScanner(RuntimeState* state, RuntimeProfile* profile,
                               const TBrokerScanRangeParams& params,
                               const std::vector<TBrokerRangeDesc>& ranges,
                               const std::vector<TNetworkAddress>& broker_addresses,
                               const std::vector<TExpr>& pre_filter_texprs, ScannerCounter* counter)
        : BrokerScanner(state, profile, params, ranges, broker_addresses, pre_filter_texprs,
                        counter) {
    _text_converter.reset(new (std::nothrow) TextConverter('\\'));
}

VBrokerScanner::~VBrokerScanner() {}

Status VBrokerScanner::get_next(Block* output_block, bool* eof) {
    SCOPED_TIMER(_read_timer);

    const int batch_size = _state->batch_size();
    // Get batch lines
    int slot_num = _src_slot_descs.size();
    std::vector<vectorized::MutableColumnPtr> columns(slot_num);
    for (int i = 0; i < slot_num; i++) {
        columns[i] = _src_slot_descs[i]->get_empty_mutable_column();
    }

    while (columns[0]->size() < batch_size && !_scanner_eof) {
        if (_cur_line_reader == nullptr || _cur_line_reader_eof) {
            RETURN_IF_ERROR(open_next_reader());
            // If there isn't any more reader, break this
            if (_scanner_eof) {
                continue;
            }
        }
        const uint8_t* ptr = nullptr;
        size_t size = 0;
        RETURN_IF_ERROR(_cur_line_reader->read_line(&ptr, &size, &_cur_line_reader_eof));
        if (_skip_lines > 0) {
            _skip_lines--;
            continue;
        }
        if (size == 0) {
            // Read empty row, just continue
            continue;
        }
        {
            COUNTER_UPDATE(_rows_read_counter, 1);
            SCOPED_TIMER(_materialize_timer);
            RETURN_IF_ERROR(_fill_dest_columns(Slice(ptr, size), columns));
            if (_success) {
                free_expr_local_allocations();
            }
        }
    }
    if (_scanner_eof) {
        *eof = true;
    } else {
        *eof = false;
    }
    return _fill_dest_block(output_block, columns);
}

Status VBrokerScanner::_fill_dest_block(Block* dest_block, std::vector<MutableColumnPtr>& columns) {
    if (columns.empty() || columns[0]->size() == 0) {
        return Status::OK();
    }

    std::unique_ptr<vectorized::Block> tmp_block(new vectorized::Block());
    auto n_columns = 0;
    for (const auto slot_desc : _src_slot_descs) {
        tmp_block->insert(ColumnWithTypeAndName(std::move(columns[n_columns++]),
                                                slot_desc->get_data_type_ptr(),
                                                slot_desc->col_name()));
    }
    auto old_rows = tmp_block->rows();
    // filter
    if (!_vpre_filter_ctxs.empty()) {
        for (auto vexpr_ctx : _vpre_filter_ctxs) {
            RETURN_IF_ERROR(VExprContext::filter_block(vexpr_ctx, tmp_block.get(),
                                                       _dest_tuple_desc->slots().size()));
            _counter->num_rows_unselected += old_rows - tmp_block->rows();
            old_rows = tmp_block->rows();
        }
    }

    Status status;
    // expr
    if (!_dest_vexpr_ctx.empty()) {
        *dest_block = vectorized::VExprContext::get_output_block_after_execute_exprs(
                _dest_vexpr_ctx, *tmp_block, status);
        if (UNLIKELY(dest_block->rows() == 0)) {
            _success = false;
            return status;
        }
    } else {
        *dest_block = *tmp_block;
    }

    return status;
}

Status VBrokerScanner::_fill_dest_columns(const Slice& line,
                                          std::vector<MutableColumnPtr>& columns) {
    RETURN_IF_ERROR(_line_split_to_values(line));
    if (!_success) {
        // If not success, which means we met an invalid row, return.
        return Status::OK();
    }

    int idx = 0;
    for (int i = 0; i < _split_values.size(); ++i) {
        int dest_index = idx++;

        auto src_slot_desc = _src_slot_descs[i];
        if (!src_slot_desc->is_materialized()) {
            continue;
        }

        const Slice& value = _split_values[i];
        if (is_null(value)) {
            // If _strict_mode is false, _src_slot_descs_order_by_dest size could be zero
            if (_strict_mode && (_src_slot_descs_order_by_dest[dest_index] != nullptr) &&
                !_src_tuple->is_null(
                        _src_slot_descs_order_by_dest[dest_index]->null_indicator_offset())) {
                RETURN_IF_ERROR(_state->append_error_msg_to_file(
                        [&]() -> std::string {
                            return _src_tuple_row->to_string(*(_row_desc.get()));
                        },
                        [&]() -> std::string {
                            // Type of the slot is must be Varchar in _src_tuple.
                            StringValue* raw_value = _src_tuple->get_string_slot(
                                    _src_slot_descs_order_by_dest[dest_index]->tuple_offset());
                            std::string raw_string;
                            if (raw_value != nullptr) { //is not null then get raw value
                                raw_string = raw_value->to_string();
                            }
                            fmt::memory_buffer error_msg;
                            fmt::format_to(error_msg,
                                           "column({}) value is incorrect while strict mode is {}, "
                                           "src value is {}",
                                           src_slot_desc->col_name(), _strict_mode, raw_string);
                            return error_msg.data();
                        },
                        &_scanner_eof));
                _counter->num_rows_filtered++;
                _success = false;
                return Status::OK();
            }

            if (!src_slot_desc->is_nullable()) {
                RETURN_IF_ERROR(_state->append_error_msg_to_file(
                        [&]() -> std::string {
                            return _src_tuple_row->to_string(*(_row_desc.get()));
                        },
                        [&]() -> std::string {
                            fmt::memory_buffer error_msg;
                            fmt::format_to(
                                    error_msg,
                                    "column({}) values is null while columns is not nullable",
                                    src_slot_desc->col_name());
                            return error_msg.data();
                        },
                        &_scanner_eof));
                _counter->num_rows_filtered++;
                _success = false;
                return Status::OK();
            }
            // nullable
            auto* nullable_column =
                    reinterpret_cast<vectorized::ColumnNullable*>(columns[dest_index].get());
            nullable_column->insert_data(nullptr, 0);
            continue;
        }

        RETURN_IF_ERROR(_write_text_column(value.data, value.size, src_slot_desc,
                                           &columns[dest_index], _state));
    }

    const TBrokerRangeDesc& range = _ranges.at(_next_range - 1);
    if (range.__isset.num_of_columns_from_file) {
        RETURN_IF_ERROR(_fill_columns_from_path(range.num_of_columns_from_file, range.columns_from_path, columns));
    }

    return Status::OK();
}

Status VBrokerScanner::_fill_columns_from_path(int start,
                                            const std::vector<std::string>& columns_from_path,
                                            std::vector<MutableColumnPtr>& columns) {
    // values of columns from path can not be null
    for (int i = 0; i < columns_from_path.size(); ++i) {
        int dest_index = i + start;
        auto slot_desc = _src_slot_descs.at(dest_index);
        const std::string& column_from_path = columns_from_path[i];
        RETURN_IF_ERROR(_write_text_column(const_cast<char*>(column_from_path.c_str()), column_from_path.size(),
                                           slot_desc, &columns[dest_index], _state));
    }
    return Status::OK();
}

Status VBrokerScanner::_write_text_column(char* value, int value_length, SlotDescriptor* slot,
                                          vectorized::MutableColumnPtr* column_ptr,
                                          RuntimeState* state) {
    if (!_text_converter->write_column(slot, column_ptr, value, value_length, true, false)) {
        std::stringstream ss;
        ss << "Fail to convert text value:'" << value << "' to " << slot->type() << " on column:`"
           << slot->col_name() + "`";
        return Status::InternalError(ss.str());
    }
    return Status::OK();
}
} // namespace doris::vectorized
