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

#include "vec/exprs/table_function/vexplode_numbers.h"

#include "vec/exprs/vexpr.h"

namespace doris::vectorized {

VExplodeNumbersTableFunction::VExplodeNumbersTableFunction() {
    _fn_name = "vexplode_numbers";
}

Status VExplodeNumbersTableFunction::process_init(vectorized::Block* block) {
    CHECK(_vexpr_context->root()->children().size() == 1)
            << "VExplodeSplitTableFunction must be have 1 children but have "
            << _vexpr_context->root()->children().size();

    int value_column_idx = -1;
    _vexpr_context->root()->children()[0]->execute(_vexpr_context, block, &value_column_idx);
    _value_column = block->get_by_position(value_column_idx).column;

    return Status::OK();
}

Status VExplodeNumbersTableFunction::process_row(size_t row_idx) {
    _is_current_empty = false;
    _eos = false;

    StringRef value = _value_column->get_data_at(row_idx);

    if (value.data == nullptr) {
        _is_current_empty = true;
        _cur_size = 0;
        _cur_offset = 0;
    } else {
        _cur_size = *reinterpret_cast<const int*>(value.data);
        _cur_offset = 0;
        _is_current_empty = (_cur_size == 0);
    }
    return Status::OK();
}

Status VExplodeNumbersTableFunction::process_close() {
    _value_column = nullptr;
    return Status::OK();
}

Status VExplodeNumbersTableFunction::reset() {
    _eos = false;
    _cur_offset = 0;
    return Status::OK();
}

Status VExplodeNumbersTableFunction::get_value(void** output) {
    if (_is_current_empty) {
        *output = nullptr;
    } else {
        *output = &_cur_offset;
    }
    return Status::OK();
}

Status VExplodeNumbersTableFunction::get_value_length(int64_t* length) {
    if (_is_current_empty) {
        *length = -1;
    } else {
        *length = sizeof(int);
    }
    return Status::OK();
}

} // namespace doris::vectorized
