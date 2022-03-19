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

#include "vec/exprs/table_function/vexplode_json_array.h"

#include "common/status.h"
#include "vec/exprs/vexpr.h"

namespace doris::vectorized {

VExplodeJsonArrayTableFunction::VExplodeJsonArrayTableFunction(ExplodeJsonArrayType type)
        : ExplodeJsonArrayTableFunction(type) {
    _fn_name = "vexplode_json_array";
}

Status VExplodeJsonArrayTableFunction::process_init(vectorized::Block* block) {
    CHECK(_vexpr_context->root()->children().size() == 1)
            << _vexpr_context->root()->children().size();

    int text_column_idx = -1;
    _vexpr_context->root()->children()[0]->execute(_vexpr_context, block, &text_column_idx);
    _text_column = block->get_by_position(text_column_idx).column;

    return Status::OK();
}

Status VExplodeJsonArrayTableFunction::process_row(size_t row_idx) {
    _is_current_empty = false;
    _eos = false;

    StringRef text = _text_column->get_data_at(row_idx);
    if (text.data == nullptr) {
        _is_current_empty = true;
    } else {
        rapidjson::Document document;
        document.Parse(text.data, text.size);
        if (UNLIKELY(document.HasParseError()) || !document.IsArray() ||
            document.GetArray().Size() == 0) {
            _is_current_empty = true;
        } else {
            _cur_size = _parsed_data.set_output(_type, document);
            _cur_offset = 0;
        }
    }
    return Status::OK();
}

Status VExplodeJsonArrayTableFunction::process_close() {
    _text_column = nullptr;
    return Status::OK();
}

Status VExplodeJsonArrayTableFunction::get_value_length(int64_t* length) {
    if (_is_current_empty) {
        *length = -1;
    } else {
        _parsed_data.get_value_length(_type, _cur_offset, length);
    }
    return Status::OK();
}

Status VExplodeJsonArrayTableFunction::get_value(void** output) {
    if (_is_current_empty) {
        *output = nullptr;
    } else {
        _parsed_data.get_value(_type, _cur_offset, output, true);
    }
    return Status::OK();
}

} // namespace doris::vectorized