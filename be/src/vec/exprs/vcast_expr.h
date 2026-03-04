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
#include "runtime/define_primitive_type.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type.h"
#include "vec/exprs/function_context.h"
#include "vec/exprs/vexpr.h"
#include "vec/functions/function.h"

namespace doris {
class RowDescriptor;
class RuntimeState;
class TExprNode;
namespace vectorized {
class Block;
class VExprContext;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {
class VCastExpr : public VExpr {
    ENABLE_FACTORY_CREATOR(VCastExpr);

public:
#ifdef BE_TEST
    VCastExpr() = default;
#endif
    VCastExpr(const TExprNode& node) : VExpr(node) {}
    ~VCastExpr() override = default;
    Status execute_column(VExprContext* context, const Block* block, Selector* selector,
                          size_t count, ColumnPtr& result_column) const override;
    Status prepare(RuntimeState* state, const RowDescriptor& desc, VExprContext* context) override;
    Status open(RuntimeState* state, VExprContext* context,
                FunctionContext::FunctionStateScope scope) override;
    void close(VExprContext* context, FunctionContext::FunctionStateScope scope) override;
    const std::string& expr_name() const override;
    std::string debug_string() const override;
    const DataTypePtr& get_target_type() const;

    virtual std::string cast_name() const { return "CAST"; }

    uint64_t get_digest(uint64_t seed) const override {
        auto res = VExpr::get_digest(seed);
        if (res) {
            return HashUtil::hash64(_target_data_type_name.data(), _target_data_type_name.size(),
                                    res);
        }
        return 0;
    }

protected:
    FunctionBasePtr _function;
    std::string _expr_name;

private:
    DataTypePtr _target_data_type;
    std::string _target_data_type_name;

    DataTypePtr _cast_param_data_type;

    static const constexpr char* function_name = "CAST";
};

class TryCastExpr final : public VCastExpr {
    ENABLE_FACTORY_CREATOR(TryCastExpr);

public:
#ifdef BE_TEST
    TryCastExpr() = default;
#endif

    TryCastExpr(const TExprNode& node)
            : VCastExpr(node), _original_cast_return_is_nullable(node.is_cast_nullable) {}
    Status execute_column(VExprContext* context, const Block* block, Selector* selector,
                          size_t count, ColumnPtr& result_column) const override;
    ~TryCastExpr() override = default;
    std::string cast_name() const override { return "TRY CAST"; }

private:
    DataTypePtr original_cast_return_type() const;
    template <bool original_cast_reutrn_is_nullable>
    Status single_row_execute(VExprContext* context, const ColumnWithTypeAndName& input_info,
                              ColumnPtr& return_column) const;

    //Try_cast always returns nullable,
    // but we also need the information of whether the return value of the original cast is nullable.
    bool _original_cast_return_is_nullable = false;
};

} // namespace doris::vectorized
