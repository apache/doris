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

#include "exec/exec_node.h"
#include "exprs/expr_context.h"
#include "exec/plain_text_line_reader.h"

namespace doris::vectorized {
VBrokerScanner::VBrokerScanner(RuntimeState* state, RuntimeProfile* profile,
                               const TBrokerScanRangeParams& params,
                               const std::vector<TBrokerRangeDesc>& ranges,
                               const std::vector<TNetworkAddress>& broker_addresses,
                               const std::vector<TExpr>& pre_filter_texprs, ScannerCounter* counter)
        : BrokerScanner(state, profile, params, ranges, broker_addresses, pre_filter_texprs,
                        counter) {}

Status VBrokerScanner::get_next(std::vector<MutableColumnPtr>& columns, bool* eof) {
    SCOPED_TIMER(_read_timer);

    const int batch_size = _state->batch_size();

    // Get one line
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
            RETURN_IF_ERROR(_convert_one_row(Slice(ptr, size), columns));
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
    return Status::OK();
}

Status VBrokerScanner::_convert_one_row(const Slice& line, std::vector<MutableColumnPtr>& columns) {
    RETURN_IF_ERROR(_line_to_src_tuple(line));
    if (!_success) {
        // If not success, which means we met an invalid row, return.
        return Status::OK();
    }

    return _fill_dest_columns(columns);
}

Status VBrokerScanner::_fill_dest_columns(std::vector<MutableColumnPtr>& columns) {
    // filter src tuple by preceding filter first
    if (!ExecNode::eval_conjuncts(&_pre_filter_ctxs[0], _pre_filter_ctxs.size(), _src_tuple_row)) {
        _counter->num_rows_unselected++;
        _success = false;
        return Status::OK();
    }
    // convert and fill dest tuple
    int ctx_idx = 0;
    for (auto slot_desc : _dest_tuple_desc->slots()) {
        if (!slot_desc->is_materialized()) {
            continue;
        }

        int dest_index = ctx_idx++;
        auto* column_ptr = columns[dest_index].get();

        ExprContext* ctx = _dest_expr_ctx[dest_index];
        void* value = ctx->get_value(_src_tuple_row);
        if (value == nullptr) {
            // Only when the expr return value is null, we will check the error message.
            std::string expr_error = ctx->get_error_msg();
            if (!expr_error.empty()) {
                RETURN_IF_ERROR(_state->append_error_msg_to_file(
                        [&]() -> std::string {
                            return _src_tuple_row->to_string(*(_row_desc.get()));
                        },
                        [&]() -> std::string { return expr_error; }, &_scanner_eof));
                _counter->num_rows_filtered++;
                // The ctx is reused, so must clear the error state and message.
                ctx->clear_error_msg();
                _success = false;
                return Status::OK();
            }
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
                                           slot_desc->col_name(), _strict_mode, raw_string);
                            return error_msg.data();
                        },
                        &_scanner_eof));
                _counter->num_rows_filtered++;
                _success = false;
                return Status::OK();
            }
            if (!slot_desc->is_nullable()) {
                RETURN_IF_ERROR(_state->append_error_msg_to_file(
                        [&]() -> std::string {
                            return _src_tuple_row->to_string(*(_row_desc.get()));
                        },
                        [&]() -> std::string {
                            fmt::memory_buffer error_msg;
                            fmt::format_to(
                                    error_msg,
                                    "column({}) values is null while columns is not nullable",
                                    slot_desc->col_name());
                            return error_msg.data();
                        },
                        &_scanner_eof));
                _counter->num_rows_filtered++;
                _success = false;
                return Status::OK();
            }
            auto* nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(column_ptr);
            nullable_column->insert_data(nullptr, 0);
            continue;
        }
        if (slot_desc->is_nullable()) {
            auto* nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(column_ptr);
            nullable_column->get_null_map_data().push_back(0);
            column_ptr = &nullable_column->get_nested_column();
        }
        char* value_ptr = (char*)value;
        switch (slot_desc->type().type) {
        case TYPE_BOOLEAN: {
            assert_cast<ColumnVector<UInt8>*>(column_ptr)->insert_data(value_ptr, 0);
            break;
        }
        case TYPE_TINYINT: {
            assert_cast<ColumnVector<Int8>*>(column_ptr)->insert_data(value_ptr, 0);
            break;
        }
        case TYPE_SMALLINT: {
            assert_cast<ColumnVector<Int16>*>(column_ptr)->insert_data(value_ptr, 0);
            break;
        }
        case TYPE_INT: {
            assert_cast<ColumnVector<Int32>*>(column_ptr)->insert_data(value_ptr, 0);
            break;
        }
        case TYPE_BIGINT: {
            assert_cast<ColumnVector<Int64>*>(column_ptr)->insert_data(value_ptr, 0);
            break;
        }
        case TYPE_LARGEINT: {
            assert_cast<ColumnVector<Int128>*>(column_ptr)->insert_data(value_ptr, 0);
            break;
        }
        case TYPE_FLOAT: {
            assert_cast<ColumnVector<Float32>*>(column_ptr)->insert_data(value_ptr, 0);
            break;
        }
        case TYPE_DOUBLE: {
            assert_cast<ColumnVector<Float64>*>(column_ptr)->insert_data(value_ptr, 0);
            break;
        }
        case TYPE_CHAR: {
            Slice* slice = reinterpret_cast<Slice*>(value_ptr);
            assert_cast<ColumnString*>(column_ptr)
                    ->insert_data(slice->data, strnlen(slice->data, slice->size));
            break;
        }
        case TYPE_VARCHAR:
        case TYPE_STRING: {
            Slice* slice = reinterpret_cast<Slice*>(value_ptr);
            assert_cast<ColumnString*>(column_ptr)->insert_data(slice->data, slice->size);
            break;
        }
        case TYPE_OBJECT: {
            Slice* slice = reinterpret_cast<Slice*>(value_ptr);
            // insert_default()
            auto* target_column = assert_cast<ColumnBitmap*>(column_ptr);

            target_column->insert_default();
            BitmapValue* pvalue = nullptr;
            int pos = target_column->size() - 1;
            pvalue = &target_column->get_element(pos);

            if (slice->size != 0) {
                BitmapValue value;
                value.deserialize(slice->data);
                *pvalue = std::move(value);
            } else {
                *pvalue = std::move(*reinterpret_cast<BitmapValue*>(slice->data));
            }
            break;
        }
        case TYPE_HLL: {
            Slice* slice = reinterpret_cast<Slice*>(value_ptr);
            auto* target_column = assert_cast<ColumnHLL*>(column_ptr);

            target_column->insert_default();
            HyperLogLog* pvalue = nullptr;
            int pos = target_column->size() - 1;
            pvalue = &target_column->get_element(pos);
            if (slice->size != 0) {
                HyperLogLog value;
                value.deserialize(*slice);
                *pvalue = std::move(value);
            } else {
                *pvalue = std::move(*reinterpret_cast<HyperLogLog*>(slice->data));
            }
            break;
        }
        case TYPE_DECIMALV2: {
            assert_cast<ColumnDecimal<Decimal128>*>(column_ptr)
                    ->insert_data(reinterpret_cast<char*>(value_ptr), 0);
            break;
        }
        case TYPE_DATETIME: {
            DateTimeValue value = *reinterpret_cast<DateTimeValue*>(value_ptr);
            VecDateTimeValue date;
            date.convert_dt_to_vec_dt(&value);
            assert_cast<ColumnVector<Int64>*>(column_ptr)
                    ->insert_data(reinterpret_cast<char*>(&date), 0);
            break;
        }
        case TYPE_DATE: {
            DateTimeValue value = *reinterpret_cast<DateTimeValue*>(value_ptr);
            VecDateTimeValue date;
            date.convert_dt_to_vec_dt(&value);
            assert_cast<ColumnVector<Int64>*>(column_ptr)
                    ->insert_data(reinterpret_cast<char*>(&date), 0);
            break;
        }
        default: {
            break;
        }
        }
    }
    _success = true;
    return Status::OK();
}
} // namespace doris::vectorized
