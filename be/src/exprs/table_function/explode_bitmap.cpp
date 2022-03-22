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

#include "exprs/table_function/explode_bitmap.h"

#include "exprs/expr_context.h"
#include "exprs/scalar_fn_call.h"

namespace doris {

ExplodeBitmapTableFunction::ExplodeBitmapTableFunction() {
    _fn_name = "explode_bitmap";
}

ExplodeBitmapTableFunction::~ExplodeBitmapTableFunction() {
    if (_cur_iter != nullptr) {
        delete _cur_iter;
        _cur_iter = nullptr;
    }
    if (_cur_bitmap_owned && _cur_bitmap != nullptr) {
        delete _cur_bitmap;
        _cur_bitmap = nullptr;
    }
}

Status ExplodeBitmapTableFunction::process(TupleRow* tuple_row) {
    CHECK(1 == _expr_context->root()->get_num_children())
            << _expr_context->root()->get_num_children();
    _eos = false;
    _is_current_empty = false;
    _cur_size = 0;
    _cur_offset = 0;

    StringVal bitmap_str =
            _expr_context->root()->get_child(0)->get_string_val(_expr_context, tuple_row);
    if (bitmap_str.is_null) {
        _is_current_empty = true;
    } else {
        if (bitmap_str.len == 0) {
            _cur_bitmap = reinterpret_cast<BitmapValue*>(bitmap_str.ptr);
            _cur_bitmap_owned = false;
        } else {
            _cur_bitmap = new BitmapValue((char*)bitmap_str.ptr);
            _cur_bitmap_owned = true;
        }
        _cur_size = _cur_bitmap->cardinality();
        if (_cur_size == 0) {
            _is_current_empty = true;
        } else {
            _reset_iterator();
        }
    }

    return Status::OK();
}

void ExplodeBitmapTableFunction::_reset_iterator() {
    DCHECK(_cur_bitmap->cardinality() > 0) << _cur_bitmap->cardinality();
    if (_cur_iter != nullptr) {
        delete _cur_iter;
        _cur_iter = nullptr;
    }
    _cur_iter = new BitmapValueIterator(*_cur_bitmap);
    _cur_value = **_cur_iter;
    _cur_offset = 0;
}

Status ExplodeBitmapTableFunction::reset() {
    _eos = false;
    if (!_is_current_empty) {
        _reset_iterator();
    }
    return Status::OK();
}

Status ExplodeBitmapTableFunction::get_value(void** output) {
    if (_is_current_empty) {
        *output = nullptr;
    } else {
        *output = &_cur_value;
    }
    return Status::OK();
}

Status ExplodeBitmapTableFunction::forward(bool* eos) {
    if (_is_current_empty) {
        *eos = true;
        _eos = true;
    } else {
        ++(*_cur_iter);
        ++_cur_offset;
        if (_cur_offset == _cur_size) {
            *eos = true;
            _eos = true;
        } else {
            _cur_value = **_cur_iter;
            *eos = false;
        }
    }
    return Status::OK();
}

} // namespace doris
