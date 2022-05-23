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
#include "exec/plain_text_line_reader.h"
#include "exec/text_converter.h"
#include "exprs/expr_context.h"
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

VBrokerScanner::~VBrokerScanner() = default;

Status VBrokerScanner::get_next(Block* output_block, bool* eof) {
    SCOPED_TIMER(_read_timer);
    RETURN_IF_ERROR(_init_src_block());

    const int batch_size = _state->batch_size();
    auto columns = _src_block.mutate_columns();

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

    return _fill_dest_block(output_block, eof);
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
            // nullable
            auto* nullable_column =
                    reinterpret_cast<vectorized::ColumnNullable*>(columns[dest_index].get());
            nullable_column->insert_default();
            continue;
        }

        RETURN_IF_ERROR(_write_text_column(value.data, value.size, src_slot_desc,
                                           &columns[dest_index], _state));
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
