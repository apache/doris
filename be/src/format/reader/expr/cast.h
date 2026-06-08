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

#include <string>

#include "common/object_pool.h"
#include "common/status.h"
#include "exprs/function_context.h"
#include "exprs/vexpr.h"

namespace doris {
class RowDescriptor;
class RuntimeState;
class TExprNode;
class Block;
class VExprContext;
} // namespace doris

namespace doris {

class Cast final : public VExpr {
    ENABLE_FACTORY_CREATOR(Cast);

public:
    Cast(const DataTypePtr& type) {
        _node_type = TExprNodeType::CAST_EXPR;
        _opcode = TExprOpcode::CAST;
        _data_type = type;
    }
    ~Cast() override = default;
    Status prepare(RuntimeState* state, const RowDescriptor& desc, VExprContext* context) override;
    Status open(RuntimeState* state, VExprContext* context,
                FunctionContext::FunctionStateScope scope) override;
    void close(VExprContext* context, FunctionContext::FunctionStateScope scope) override;
    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override;
    std::string debug_string() const override;
    uint64_t get_digest(uint64_t seed) const override { return 0; }
    const std::string& expr_name() const override { return _expr_name; }

private:
    Status _do_execute(VExprContext* context, const Block* block, const Selector* selector,
                       size_t count, ColumnPtr& result_column) const;
    std::string _expr_name;
    FunctionBasePtr _function;
};
} // namespace doris
