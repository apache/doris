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

#pragma once

#include "exprs/table_function/explode_split.h"
#include "gutil/strings/split.h"
#include "gutil/strings/stringpiece.h"
#include "runtime/string_value.h"
#include "vec/columns/column.h"
#include "vec/exprs/vexpr.h"

namespace doris::vectorized {

template <typename Impl>
class VExplodeSplitTableFunctionBase : public ExplodeSplitTableFunction {
public:
    VExplodeSplitTableFunctionBase() { _fn_name = Impl::name; }
    ~VExplodeSplitTableFunctionBase() override = default;

    Status open() override { return Status::OK(); }

    Status process_init(vectorized::Block* block) override {
        CHECK(_vexpr_context->root()->children().size() == 2)
                << "VExplodeSplitTableFunction must be have 2 children but have "
                << _vexpr_context->root()->children().size();

        int text_column_idx = -1;
        int delimiter_column_idx = -1;

        _vexpr_context->root()->children()[0]->execute(_vexpr_context, block, &text_column_idx);
        _vexpr_context->root()->children()[1]->execute(_vexpr_context, block,
                                                       &delimiter_column_idx);

        _text_column = block->get_by_position(text_column_idx).column;
        _delimiter_column = block->get_by_position(delimiter_column_idx).column;

        return Status::OK();
    }

    Status process_row(size_t row_idx) override {
        _is_current_empty = false;
        _eos = false;

        StringRef text = _text_column->get_data_at(row_idx);
        StringRef delimiter = _delimiter_column->get_data_at(row_idx);

        if (text.data == nullptr) {
            _is_current_empty = true;
            _cur_size = 0;
            _cur_offset = 0;
        } else {
            //TODO: implement non-copy split string reference
            _backup = strings::Split(StringPiece((char*)text.data, text.size),
                                     StringPiece((char*)delimiter.data, delimiter.size),
                                     typename Impl::filter());

            _cur_size = _backup.size();
            _cur_offset = 0;
            _is_current_empty = (_cur_size == 0);
        }
        return Status::OK();
    }

    Status process_close() override {
        _text_column = nullptr;
        _delimiter_column = nullptr;
        return Status::OK();
    }

    Status get_value(void** output) override {
        if (_is_current_empty) {
            *output = nullptr;
        } else {
            *output = _backup[_cur_offset].data();
        }
        return Status::OK();
    }

    Status get_value_length(int64_t* length) override {
        if (_is_current_empty) {
            *length = -1;
        } else {
            *length = _backup[_cur_offset].length();
        }
        return Status::OK();
    }

private:
    ColumnPtr _text_column;
    ColumnPtr _delimiter_column;
};

struct EmptyFilter {
    bool operator()(StringPiece str) { return !str.empty(); }
};

struct VExplodeSplitImpl {
    static constexpr auto name = "vexplode_split";
    using filter = strings::internal::NoFilter;
};

struct VExplodeSplitNonEmptyImpl {
    static constexpr auto name = "vexplode_split_non_empty";
    using filter = EmptyFilter;
};

using VExplodeSplitTableFunction = VExplodeSplitTableFunctionBase<VExplodeSplitImpl>;

using VExplodeSplitNonEmptyTableFunction =
        VExplodeSplitTableFunctionBase<VExplodeSplitNonEmptyImpl>;

} // namespace doris::vectorized
