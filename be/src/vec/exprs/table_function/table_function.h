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

namespace doris::vectorized {

constexpr auto COMBINATOR_SUFFIX_OUTER = "_outer";

class TableFunction {
public:
    virtual ~TableFunction() = default;

    virtual Status prepare() { return Status::OK(); }

    virtual Status open() { return Status::OK(); }

    virtual Status process_init(Block* block, RuntimeState* state) = 0;

    virtual void process_row(size_t row_idx) {
        if (!_is_const) {
            _cur_size = 0;
        }
        reset();
    }

    // only used for vectorized.
    virtual void process_close() = 0;

    virtual void reset() {
        _eos = false;
        _cur_offset = 0;
    }

    virtual void get_value(MutableColumnPtr& column) = 0;

    virtual int get_value(MutableColumnPtr& column, int max_step) {
        max_step = std::max(1, std::min(max_step, (int)(_cur_size - _cur_offset)));
        int i = 0;
        for (; i < max_step && !eos(); i++) {
            get_value(column);
            forward();
        }
        return i;
    }

    virtual Status close() { return Status::OK(); }

    virtual void forward(int step = 1) {
        if (current_empty()) {
            _eos = true;
        } else {
            _cur_offset += step;
            if (_cur_offset >= _cur_size) {
                _eos = true;
            }
        }
    }

    std::string name() const { return _fn_name; }
    bool eos() const { return _eos; }

    void set_expr_context(const VExprContextSPtr& expr_context) { _expr_context = expr_context; }
    void set_nullable() { _is_nullable = true; }

    bool is_outer() const { return _is_outer; }
    void set_outer() {
        if (is_outer()) {
            return;
        }
        _is_outer = true;
        _fn_name += COMBINATOR_SUFFIX_OUTER;
    }

    bool current_empty() const { return _cur_size == 0; }

protected:
    std::string _fn_name;
    VExprContextSPtr _expr_context = nullptr;
    // true if there is no more data can be read from this function.
    bool _eos = false;
    // the position of current cursor
    int64_t _cur_offset = 0;
    // the size of current result
    int64_t _cur_size = 0;
    // set _is_outer to false for explode function, and should not return tuple while array is null or empty
    bool _is_outer = false;

    bool _is_nullable = false;
    bool _is_const = false;
};
} // namespace doris::vectorized
