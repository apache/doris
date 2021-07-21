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
#include "vec/functions/function.h"

namespace doris::vectorized {
class VCastExpr final: public VExpr {
public:
    VCastExpr(const TExprNode& node) : VExpr(node) {}
    ~VCastExpr() = default;
    virtual doris::Status execute(doris::vectorized::Block* block, int* result_column_id);
    virtual doris::Status prepare(doris::RuntimeState* state, const doris::RowDescriptor& desc,
                                  VExprContext* context);
    virtual doris::Status open(doris::RuntimeState* state, VExprContext* context);
    virtual void close(doris::RuntimeState* state, VExprContext* context);
    virtual VExpr* clone(doris::ObjectPool* pool) const override {
        return pool->add(new VCastExpr(*this));
    }
    virtual const std::string& expr_name() const override;

private:
    FunctionBasePtr _function;
    std::string _expr_name;

    DataTypePtr _target_data_type;
    std::string _target_data_type_name;

    DataTypePtr _cast_param_data_type;
    ColumnPtr _cast_param;

private:
    static const constexpr char* function_name = "CAST";
};
} // namespace doris::vectorized
