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

#include "common/status.h"

namespace doris {

// TODO: think about how to manager memeory consumption of table functions.
// Currently, the memory allocated from table function is from malloc directly.
class TableFunctionState {
};

class ExprContext;
class TupleRow;
class TableFunction {
public:
    virtual ~TableFunction() {}

    virtual Status prepare() = 0;
    virtual Status open() = 0;
    virtual Status process(TupleRow* tuple_row) = 0;
    virtual Status reset() = 0;
    virtual Status get_value(void** output) = 0;
    virtual Status close() = 0;

    virtual Status forward(bool *eos) = 0;

public:
    bool eos() const { return _eos; }

    void set_expr_context(ExprContext* expr_context) {
        _expr_context = expr_context;
    }

protected:
    std::string _fn_name;
    ExprContext* _expr_context;
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
