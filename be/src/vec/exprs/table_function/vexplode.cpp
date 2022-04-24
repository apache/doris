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

#include "vec/exprs/table_function/vexplode.h"

#include "vec/exprs/vexpr.h"

namespace doris::vectorized {

VExplodeTableFunction::VExplodeTableFunction() {
    _fn_name = "vexplode";
}

Status VExplodeTableFunction::process_init(vectorized::Block* block) {
    CHECK(_vexpr_context->root()->children().size() == 1)
            << "VExplodeTableFunction must be have 1 children but have "
            << _vexpr_context->root()->children().size();

    int value_column_idx = -1;
    _vexpr_context->root()->children()[0]->execute(_vexpr_context, block, &value_column_idx);

    if (block->get_by_position(value_column_idx).column->is_nullable()) {
        auto array_nullable_column = check_and_get_column<ColumnNullable>(
                *block->get_by_position(value_column_idx).column);
        _array_null_map = array_nullable_column->get_null_map_column().get_data().data();
        _array_column =
                check_and_get_column<ColumnArray>(array_nullable_column->get_nested_column_ptr());
    } else {
        _array_null_map = nullptr;
        _array_column =
                check_and_get_column<ColumnArray>(*block->get_by_position(value_column_idx).column);
    }
    if (!_array_column) {
        return Status::NotSupported("column type " +
                                    block->get_by_position(value_column_idx).column->get_name() +
                                    " not supported now");
    }

    return Status::OK();
}

Status VExplodeTableFunction::process_row(size_t row_idx) {
    DCHECK(row_idx < _array_column->size());
    _is_current_empty = false;
    _eos = false;

    if (_array_null_map && _array_null_map[row_idx]) {
        _is_current_empty = true;
        _cur_size = 0;
        _cur_offset = 0;
        _pos = 0;
    } else {
        _cur_size =
                _array_column->get_offsets()[row_idx] - _array_column->get_offsets()[row_idx - 1];
        _cur_offset = 0;
        _is_current_empty = (_cur_size == 0);
        _pos = _array_column->get_offsets()[row_idx - 1];
    }
    return Status::OK();
}

Status VExplodeTableFunction::process_close() {
    _array_column = nullptr;
    _array_null_map = nullptr;
    _pos = 0;
    return Status::OK();
}

Status VExplodeTableFunction::reset() {
    _eos = false;
    _cur_offset = 0;
    return Status::OK();
}

Status VExplodeTableFunction::get_value(void** output) {
    if (_is_current_empty) {
        *output = nullptr;
        return Status::OK();
    }

    *output = const_cast<char*>(_array_column->get_data().get_data_at(_pos + _cur_offset).data);
    return Status::OK();
}

Status VExplodeTableFunction::get_value_length(int64_t* length) {
    if (_is_current_empty) {
        *length = -1;
        return Status::OK();
    }

    *length = _array_column->get_data().get_data_at(_pos + _cur_offset).size;
    return Status::OK();
}

} // namespace doris::vectorized
