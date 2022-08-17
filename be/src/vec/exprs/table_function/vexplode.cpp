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

#include "common/status.h"
#include "vec/exprs/vexpr.h"

namespace doris::vectorized {

VExplodeTableFunction::VExplodeTableFunction() {
    _fn_name = "vexplode";
}

Status VExplodeTableFunction::process_init(vectorized::Block* block) {
    CHECK(_vexpr_context->root()->children().size() == 1)
            << "VExplodeTableFunction only support 1 child but has "
            << _vexpr_context->root()->children().size();

    int value_column_idx = -1;
    RETURN_IF_ERROR(_vexpr_context->root()->children()[0]->execute(_vexpr_context, block,
                                                                   &value_column_idx));

    _array_column =
            block->get_by_position(value_column_idx).column->convert_to_full_column_if_const();

    if (!extract_column_array_info(*_array_column, _detail)) {
        return Status::NotSupported("column type {} not supported now",
                                    block->get_by_position(value_column_idx).column->get_name());
    }

    return Status::OK();
}

Status VExplodeTableFunction::process_row(size_t row_idx) {
    DCHECK(row_idx < _array_column->size());
    _is_current_empty = false;
    _eos = false;
    _cur_offset = 0;
    _array_offset = (*_detail.offsets_ptr)[row_idx - 1];
    _cur_size = (*_detail.offsets_ptr)[row_idx] - _array_offset;

    // array is NULL, or array is empty
    if (_cur_size == 0 || (_detail.array_nullmap_data && _detail.array_nullmap_data[row_idx])) {
        _is_current_empty = true;
    }

    return Status::OK();
}

Status VExplodeTableFunction::process_close() {
    _array_column = nullptr;
    _detail.reset();
    _array_offset = 0;
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

    size_t pos = _array_offset + _cur_offset;
    if (_detail.nested_nullmap_data && _detail.nested_nullmap_data[pos]) {
        *output = nullptr;
    } else {
        *output = const_cast<char*>(_detail.nested_col->get_data_at(pos).data);
    }

    return Status::OK();
}

Status VExplodeTableFunction::get_value_length(int64_t* length) {
    if (_is_current_empty) {
        *length = -1;
        return Status::OK();
    }

    size_t pos = _array_offset + _cur_offset;
    if (_detail.nested_nullmap_data && _detail.nested_nullmap_data[pos]) {
        *length = 0;
    } else {
        *length = _detail.nested_col->get_data_at(pos).size;
    }

    return Status::OK();
}

} // namespace doris::vectorized
