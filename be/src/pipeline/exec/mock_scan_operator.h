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

#include "pipeline/exec/scan_operator.h"

#ifdef BE_TEST
namespace doris::pipeline {

class MockScanOperatorX;
class MockScanLocalState final : public ScanLocalState<MockScanLocalState> {
public:
    using Parent = MockScanOperatorX;
    friend class MockScanOperatorX;
    ENABLE_FACTORY_CREATOR(MockScanLocalState);
    MockScanLocalState(RuntimeState* state, OperatorXBase* parent)
            : ScanLocalState(state, parent) {}

protected:
    bool _is_key_column(const std::string& col_name) override { return true; }

private:
    PushDownType _should_push_down_bloom_filter() const override {
        return PushDownType::ACCEPTABLE;
    }

    PushDownType _should_push_down_bitmap_filter() const override {
        return PushDownType::ACCEPTABLE;
    }

    bool _should_push_down_common_expr() override { return true; }
    PushDownType _should_push_down_topn_filter() const override { return PushDownType::ACCEPTABLE; }

    PushDownType _should_push_down_is_null_predicate(
            vectorized::VectorizedFnCall* fn_call) const override {
        return fn_call->fn().name.function_name == "is_null_pred" ||
                               fn_call->fn().name.function_name == "is_not_null_pred"
                       ? PushDownType::ACCEPTABLE
                       : PushDownType::UNACCEPTABLE;
    }
    PushDownType _should_push_down_in_predicate() const override {
        return PushDownType::ACCEPTABLE;
    }
    PushDownType _should_push_down_binary_predicate(
            vectorized::VectorizedFnCall* fn_call, vectorized::VExprContext* expr_ctx,
            vectorized::Field& constant_val, const std::set<std::string> fn_name) const override {
        if (!fn_name.contains(fn_call->fn().name.function_name)) {
            return PushDownType::UNACCEPTABLE;
        }
        const auto& children = fn_call->children();
        DCHECK(children.size() == 2);
        DCHECK_EQ(children[0]->node_type(), TExprNodeType::SLOT_REF);
        if (children[1]->is_constant()) {
            std::shared_ptr<ColumnPtrWrapper> const_col_wrapper;
            THROW_IF_ERROR(children[1]->get_const_col(expr_ctx, &const_col_wrapper));
            const auto* const_column = assert_cast<const vectorized::ColumnConst*>(
                    const_col_wrapper->column_ptr.get());
            constant_val = const_column->operator[](0);
            return PushDownType::ACCEPTABLE;
        } else {
            // only handle constant value
            return PushDownType::UNACCEPTABLE;
        }
    }
};

class MockScanOperatorX final : public ScanOperatorX<MockScanLocalState> {
public:
    friend class OlapScanLocalState;
    MockScanOperatorX() = default;
};
} // namespace doris::pipeline
#endif