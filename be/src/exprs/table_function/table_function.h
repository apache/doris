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

#include <fmt/core.h>
#include <stddef.h>

#include "common/status.h"
#include "vec/core/block.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {

// TODO: think about how to manager memeory consumption of table functions.
// Currently, the memory allocated from table function is from malloc directly.
class TableFunctionState {};

class ExprContext;
class TupleRow;
class TableFunction {
public:
    virtual ~TableFunction() {}

    virtual Status prepare() { return Status::OK(); }

    virtual Status open() { return Status::OK(); }

    virtual Status process(TupleRow* tuple_row) {
        return Status::NotSupported(fmt::format("table function {} not supported now.", _fn_name));
    }

    // only used for vectorized.
    virtual Status process_init(vectorized::Block* block) {
        return Status::NotSupported(
                fmt::format("vectorized table function {} not supported now.", _fn_name));
    }

    // only used for vectorized.
    virtual Status process_row(size_t row_idx) {
        return Status::NotSupported(
                fmt::format("vectorized table function {} not supported now.", _fn_name));
    }

    // only used for vectorized.
    virtual Status process_close() {
        return Status::NotSupported(
                fmt::format("vectorized table function {} not supported now.", _fn_name));
    }

    virtual Status reset() = 0;

    virtual Status get_value(void** output) = 0;

    // only used for vectorized.
    virtual Status get_value_length(int64_t* length) {
        *length = -1;
        return Status::OK();
    }

    virtual Status close() { return Status::OK(); }

    virtual Status forward(bool* eos) {
        if (_is_current_empty) {
            *eos = true;
            _eos = true;
        } else {
            ++_cur_offset;
            if (_cur_offset == _cur_size) {
                *eos = true;
                _eos = true;
            } else {
                *eos = false;
            }
        }
        return Status::OK();
    }

    std::string name() const { return _fn_name; }
    bool eos() const { return _eos; }

    void set_expr_context(ExprContext* expr_context) { _expr_context = expr_context; }
    void set_vexpr_context(vectorized::VExprContext* vexpr_context) {
        _vexpr_context = vexpr_context;
    }

protected:
    std::string _fn_name;
    ExprContext* _expr_context = nullptr;
    vectorized::VExprContext* _vexpr_context = nullptr;
    // true if there is no more data can be read from this function.
    bool _eos = false;
    // true means the function result set from current row is empty(eg, source value is null or empty).
    // so that when calling reset(), we can do nothing and keep eos as true.
    bool _is_current_empty = false;
    // the position of current cursor
    int64_t _cur_offset = 0;
    // the size of current result
    int64_t _cur_size = 0;
};

} // namespace doris
