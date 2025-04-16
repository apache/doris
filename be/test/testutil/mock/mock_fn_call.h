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
#include <memory>
#include <string>

#include "common/status.h"
#include "vec/exprs/vectorized_fn_call.h"
#include "vec/exprs/vexpr.h"

namespace doris::vectorized {
class Block;
class VExprContext;
// use to mock a slot ref expr
class MockFnCall final : public VectorizedFnCall {
public:
    MockFnCall(const std::string fn_name) {
        _fn.name.function_name = fn_name;
        _function_name = fn_name;
    }

    Status get_const_col(VExprContext* context,
                         std::shared_ptr<ColumnPtrWrapper>* column_wrapper) override {
        *column_wrapper = _mock_const_expr_col;
        return Status::OK();
    }

    Status execute(VExprContext* context, Block* block, int* result_column_id) override {
        return Status::OK();
    }
    Status prepare(RuntimeState* state, const RowDescriptor& desc, VExprContext* context) override {
        _prepare_finished = true;
        return Status::OK();
    }
    Status open(RuntimeState* state, VExprContext* context,
                FunctionContext::FunctionStateScope scope) override {
        _open_finished = true;
        return Status::OK();
    }

    void set_const_expr_col(ColumnPtr col) {
        _mock_const_expr_col = std::make_shared<ColumnPtrWrapper>(col);
    }

    static std::shared_ptr<MockFnCall> create(const std::string& fn_name) {
        return std::make_shared<MockFnCall>(fn_name);
    }

    bool is_constant() const override { return _mock_is_constant; }

private:
    const std::string _name = "MockFnCall";
    std::shared_ptr<ColumnPtrWrapper> _mock_const_expr_col;
    bool _mock_is_constant = false;
};

} // namespace doris::vectorized
