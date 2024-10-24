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

#include "vec/exprs/table_function/vexplode_split.h"

#include <glog/logging.h>

#include <algorithm>
#include <iterator>
#include <ostream>

#include "common/status.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris::vectorized {

VExplodeSplitTableFunction::VExplodeSplitTableFunction() {
    _fn_name = "vexplode_split";
}

Status VExplodeSplitTableFunction::open() {
    return Status::OK();
}

Status VExplodeSplitTableFunction::process_init(Block* block, RuntimeState* state) {
    CHECK(_expr_context->root()->children().size() == 2)
            << "VExplodeSplitTableFunction must be have 2 children but have "
            << _expr_context->root()->children().size();

    int text_column_idx = -1;
    int delimiter_column_idx = -1;

    RETURN_IF_ERROR(_expr_context->root()->children()[0]->execute(_expr_context.get(), block,
                                                                  &text_column_idx));
    RETURN_IF_ERROR(_expr_context->root()->children()[1]->execute(_expr_context.get(), block,
                                                                  &delimiter_column_idx));

    // dispose test column
    _text_column =
            block->get_by_position(text_column_idx).column->convert_to_full_column_if_const();
    if (_text_column->is_nullable()) {
        const auto& column_null = assert_cast<const ColumnNullable&>(*_text_column);
        _test_null_map = column_null.get_null_map_data().data();
        _real_text_column = &assert_cast<const ColumnString&>(column_null.get_nested_column());
    } else {
        _real_text_column = &assert_cast<const ColumnString&>(*_text_column);
    }

    // dispose delimiter column
    auto& delimiter_const_column = block->get_by_position(delimiter_column_idx).column;
    if (is_column_const(*delimiter_const_column)) {
        _delimiter = delimiter_const_column->get_data_at(0);
        if (_delimiter.empty()) {
            return Status::InvalidArgument(
                    "explode_split(test, delimiter) delimiter column must be not empty");
        }
    } else {
        return Status::NotSupported(
                "explode_split(test, delimiter) delimiter column must be const");
    }

    return Status::OK();
}

void VExplodeSplitTableFunction::process_row(size_t row_idx) {
    TableFunction::process_row(row_idx);

    if (!(_test_null_map && _test_null_map[row_idx]) && _delimiter.data != nullptr) {
        // TODO: use the function to be better string_view/StringRef split
        auto split = [](std::string_view strv, std::string_view delims = " ") {
            std::vector<StringRef> output;
            const auto* first = strv.begin();
            const auto* last = strv.end();

            do {
                const auto* second =
                        std::search(first, last, std::cbegin(delims), std::cend(delims));
                if (first != second) {
                    auto view = strv.substr(std::distance(strv.begin(), first),
                                            std::distance(first, second));
                    output.emplace_back(view.data(), view.length());
                } else {
                    output.emplace_back("", 0);
                }
                first = std::next(second, delims.size());

                if (second == last) {
                    break;
                }
            } while (first != last);

            return output;
        };
        _backup = split(_real_text_column->get_data_at(row_idx), _delimiter);

        _cur_size = _backup.size();
    }
}

void VExplodeSplitTableFunction::process_close() {
    _text_column = nullptr;
    _real_text_column = nullptr;
    _test_null_map = nullptr;
    _delimiter = {};
}

void VExplodeSplitTableFunction::get_same_many_values(MutableColumnPtr& column, int length) {
    if (current_empty()) {
        column->insert_many_defaults(length);
    } else {
        column->insert_many_data(_backup[_cur_offset].data, _backup[_cur_offset].size, length);
    }
}

int VExplodeSplitTableFunction::get_value(doris::vectorized::MutableColumnPtr& column,
                                          int max_step) {
    max_step = std::min(max_step, (int)(_cur_size - _cur_offset));
    // should dispose the empty status, forward one step
    if (current_empty()) {
        column->insert_default();
        max_step = 1;
    } else {
        ColumnString* target = nullptr;
        if (_is_nullable) {
            target = assert_cast<ColumnString*>(
                    assert_cast<ColumnNullable*>(column.get())->get_nested_column_ptr().get());
            assert_cast<ColumnUInt8*>(
                    assert_cast<ColumnNullable*>(column.get())->get_null_map_column_ptr().get())
                    ->insert_many_defaults(max_step);
        } else {
            target = assert_cast<ColumnString*>(column.get());
        }
        target->insert_many_strings(_backup.data() + _cur_offset, max_step);
    }
    TableFunction::forward(max_step);
    return max_step;
}
} // namespace doris::vectorized
