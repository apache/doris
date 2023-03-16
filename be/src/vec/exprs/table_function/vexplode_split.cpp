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

#include "common/status.h"
#include "gutil/strings/split.h"
#include "vec/columns/column.h"
#include "vec/exprs/vexpr.h"

namespace doris::vectorized {

VExplodeSplitTableFunction::VExplodeSplitTableFunction() {
    _fn_name = "vexplode_split";
}

Status VExplodeSplitTableFunction::open() {
    return Status::OK();
}

Status VExplodeSplitTableFunction::reset() {
    _eos = false;
    if (!_is_current_empty) {
        _cur_offset = 0;
    }
    return Status::OK();
}

Status VExplodeSplitTableFunction::process_init(vectorized::Block* block) {
    CHECK(_vexpr_context->root()->children().size() == 2)
            << "VExplodeSplitTableFunction must be have 2 children but have "
            << _vexpr_context->root()->children().size();

    int text_column_idx = -1;
    int delimiter_column_idx = -1;

    RETURN_IF_ERROR(_vexpr_context->root()->children()[0]->execute(_vexpr_context, block,
                                                                   &text_column_idx));
    RETURN_IF_ERROR(_vexpr_context->root()->children()[1]->execute(_vexpr_context, block,
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
    } else {
        return Status::NotSupported(
                "explode_split(test, delimiter) delimiter column must be const");
    }

    return Status::OK();
}

Status VExplodeSplitTableFunction::process_row(size_t row_idx) {
    _is_current_empty = false;
    _eos = false;

    if ((_test_null_map and _test_null_map[row_idx]) || _delimiter.data == nullptr) {
        _is_current_empty = true;
        _cur_size = 0;
        _cur_offset = 0;
    } else {
        // TODO: use the function to be better string_view/StringRef split
        auto split = [](std::string_view strv, std::string_view delims = " ") {
            std::vector<std::string_view> output;
            auto first = strv.begin();
            auto last = strv.end();

            do {
                const auto second =
                        std::search(first, last, std::cbegin(delims), std::cend(delims));
                if (first != second) {
                    output.emplace_back(strv.substr(std::distance(strv.begin(), first),
                                                    std::distance(first, second)));
                    first = std::next(second);
                } else {
                    output.emplace_back("", 0);
                    first = std::next(second, delims.size());
                }

                if (second == last) {
                    break;
                }
            } while (first != last);

            return output;
        };
        _backup = split(_real_text_column->get_data_at(row_idx), _delimiter);

        _cur_size = _backup.size();
        _cur_offset = 0;
        _is_current_empty = (_cur_size == 0);
    }
    return Status::OK();
}

Status VExplodeSplitTableFunction::process_close() {
    _text_column = nullptr;
    _real_text_column = nullptr;
    _test_null_map = nullptr;
    _delimiter = {};
    return Status::OK();
}

Status VExplodeSplitTableFunction::get_value(void** output) {
    if (_is_current_empty) {
        *output = nullptr;
    } else {
        *output = const_cast<char*>(_backup[_cur_offset].data());
    }
    return Status::OK();
}

Status VExplodeSplitTableFunction::get_value_length(int64_t* length) {
    if (_is_current_empty) {
        *length = -1;
    } else {
        *length = _backup[_cur_offset].length();
    }
    return Status::OK();
}

} // namespace doris::vectorized
