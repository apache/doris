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

#include "exprs/table_function/explode_split.h"

#include "exprs/expr_context.h"
#include "exprs/scalar_fn_call.h"
#include "gutil/strings/split.h"

namespace doris {

ExplodeSplitTableFunction::ExplodeSplitTableFunction() {
    _fn_name = "explode_split";
}

ExplodeSplitTableFunction::~ExplodeSplitTableFunction() {}

Status ExplodeSplitTableFunction::open() {
    ScalarFnCall* fn_call = reinterpret_cast<ScalarFnCall*>(_expr_context->root());
    FunctionContext* fn_ctx = _expr_context->fn_context(fn_call->get_fn_context_index());
    CHECK(2 == fn_ctx->get_num_constant_args()) << fn_ctx->get_num_constant_args();
    // check if the delimiter argument(the 2nd arg) is constant.
    // if yes, cache it
    if (fn_ctx->is_arg_constant(1)) {
        _is_delimiter_constant = true;
        StringVal* delimiter = reinterpret_cast<StringVal*>(fn_ctx->get_constant_arg(1));
        _const_delimter = StringPiece((char*)delimiter->ptr, delimiter->len);
    }
    return Status::OK();
}

Status ExplodeSplitTableFunction::process(TupleRow* tuple_row) {
    CHECK(2 == _expr_context->root()->get_num_children())
            << _expr_context->root()->get_num_children();
    _is_current_empty = false;
    _eos = false;

    _data.clear();
    StringVal text = _expr_context->root()->get_child(0)->get_string_val(_expr_context, tuple_row);
    if (text.is_null) {
        _is_current_empty = true;
        _cur_size = 0;
        _cur_offset = 0;
    } else {
        if (_is_delimiter_constant) {
            _backup = strings::Split(StringPiece((char*)text.ptr, text.len), _const_delimter);
        } else {
            StringVal delimiter =
                    _expr_context->root()->get_child(1)->get_string_val(_expr_context, tuple_row);
            _backup = strings::Split(StringPiece((char*)text.ptr, text.len),
                                     StringPiece((char*)delimiter.ptr, delimiter.len));
        }
        for (const std::string& str : _backup) {
            _data.emplace_back(str);
        }
        _cur_size = _backup.size();
        _cur_offset = 0;
        _is_current_empty = (_cur_size == 0);
    }
    return Status::OK();
}

Status ExplodeSplitTableFunction::reset() {
    _eos = false;
    if (!_is_current_empty) {
        _cur_offset = 0;
    }
    return Status::OK();
}

Status ExplodeSplitTableFunction::get_value(void** output) {
    if (_is_current_empty) {
        *output = nullptr;
    } else {
        *output = &_data[_cur_offset];
    }
    return Status::OK();
}
} // namespace doris
