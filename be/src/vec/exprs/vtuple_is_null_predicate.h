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

#include "vec/exprs/vexpr.h"

namespace doris::vectorized {
class VTupleIsNullPredicate final : public VExpr {
public:
    explicit VTupleIsNullPredicate(const TExprNode& node);
    ~VTupleIsNullPredicate() override = default;
    doris::Status execute(VExprContext* context, doris::vectorized::Block* block,
                          int* result_column_id) override;
    doris::Status prepare(doris::RuntimeState* state, const doris::RowDescriptor& desc,
                          VExprContext* context) override;

    VExpr* clone(doris::ObjectPool* pool) const override {
        return pool->add(new VTupleIsNullPredicate(*this));
    }

    [[nodiscard]] bool is_constant() const override { return false; }

    [[nodiscard]] const std::string& expr_name() const override;

    virtual std::string debug_string() const override;

private:
    std::string _expr_name;
    bool _is_left_null_side;
    uint32_t _column_to_check;

private:
    static const constexpr char* function_name = "tuple_is_null";
};
} // namespace doris::vectorized
