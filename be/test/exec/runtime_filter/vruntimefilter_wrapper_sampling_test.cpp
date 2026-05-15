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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "exec/runtime_filter/runtime_filter_selectivity.h"
#include "exec/runtime_filter/runtime_filter_test_utils.h"
#include "exprs/vexpr_context.h"
#include "exprs/vruntimefilter_wrapper.h"

namespace doris {

// Minimal VExpr implementation for testing VRuntimeFilterWrapper in isolation.
class StubVExpr : public VExpr {
public:
    StubVExpr() : VExpr(make_texpr_node()) {}

    const std::string& expr_name() const override {
        static const std::string name = "StubVExpr";
        return name;
    }

    Status execute(VExprContext*, Block*, int*) const override { return Status::OK(); }

    Status execute_column(VExprContext*, const Block*, Selector*, size_t,
                          ColumnPtr&) const override {
        return Status::OK();
    }

    // SLOT_REF is not a constant — without this override, VExpr::is_constant()
    // returns true for a leaf node (no children), causing get_const_col() to
    // DCHECK-fail on the second open() call.
    bool is_constant() const override { return false; }

private:
    static TExprNode make_texpr_node() {
        return TExprNodeBuilder(TExprNodeType::SLOT_REF,
                                TTypeDescBuilder()
                                        .set_types(TTypeNodeBuilder()
                                                           .set_type(TTypeNodeType::SCALAR)
                                                           .set_scalar_type(TPrimitiveType::INT)
                                                           .build())
                                        .build(),
                                0)
                .build();
    }
};

class VRuntimeFilterWrapperSamplingTest : public RuntimeFilterTest {};

// Test that VRuntimeFilterWrapper stores and propagates sampling_frequency
// through open() to VExprContext. This is the core fix for the bug where
// sampling_frequency was lost when _append_rf_into_conjuncts creates a new
// VExprContext via VExprContext::create_shared(expr).
TEST_F(VRuntimeFilterWrapperSamplingTest, open_propagates_sampling_frequency) {
    auto stub = std::make_shared<StubVExpr>();
    auto node = TExprNodeBuilder(TExprNodeType::SLOT_REF,
                                 TTypeDescBuilder()
                                         .set_types(TTypeNodeBuilder()
                                                            .set_type(TTypeNodeType::SCALAR)
                                                            .set_scalar_type(TPrimitiveType::INT)
                                                            .build())
                                         .build(),
                                 0)
                        .build();

    const int expected_frequency = 32;
    auto wrapper = VRuntimeFilterWrapper::create_shared(node, stub, 0.4, false, /*filter_id=*/1,
                                                        expected_frequency);

    // Simulate the VExprContext recreation that happens in _append_rf_into_conjuncts.
    // A fresh VExprContext has default sampling_frequency = DISABLE_SAMPLING (-1).
    auto context = std::make_shared<VExprContext>(wrapper);
    ASSERT_EQ(context->get_runtime_filter_selectivity().maybe_always_true_can_ignore(), false);

    RowDescriptor row_desc;
    ASSERT_TRUE(wrapper->prepare(_runtime_states[0].get(), row_desc, context.get()).ok());
    ASSERT_TRUE(
            wrapper->open(_runtime_states[0].get(), context.get(), FunctionContext::FRAGMENT_LOCAL)
                    .ok());

    // After open(), sampling_frequency should be propagated from VRuntimeFilterWrapper
    // to VExprContext. Verify by accumulating low-selectivity data and checking
    // that always_true can now be detected.
    auto& selectivity = context->get_runtime_filter_selectivity();
    selectivity.update_judge_selectivity(1, 2000, 50000, 0.1);
    EXPECT_TRUE(selectivity.maybe_always_true_can_ignore());
}

// Test that default sampling_frequency (DISABLE_SAMPLING) disables the always_true
// optimization, matching the behavior when disable_always_true_logic is set.
TEST_F(VRuntimeFilterWrapperSamplingTest, default_sampling_frequency_disables_optimization) {
    auto stub = std::make_shared<StubVExpr>();
    auto node = TExprNodeBuilder(TExprNodeType::SLOT_REF,
                                 TTypeDescBuilder()
                                         .set_types(TTypeNodeBuilder()
                                                            .set_type(TTypeNodeType::SCALAR)
                                                            .set_scalar_type(TPrimitiveType::INT)
                                                            .build())
                                         .build(),
                                 0)
                        .build();

    // No sampling_frequency argument - uses default DISABLE_SAMPLING
    auto wrapper = VRuntimeFilterWrapper::create_shared(node, stub, 0.4, false, /*filter_id=*/1);

    auto context = std::make_shared<VExprContext>(wrapper);
    RowDescriptor row_desc;
    ASSERT_TRUE(wrapper->prepare(_runtime_states[0].get(), row_desc, context.get()).ok());
    ASSERT_TRUE(
            wrapper->open(_runtime_states[0].get(), context.get(), FunctionContext::FRAGMENT_LOCAL)
                    .ok());

    // Even with low-selectivity data, always_true should NOT be detected
    // because sampling is disabled
    auto& selectivity = context->get_runtime_filter_selectivity();
    selectivity.update_judge_selectivity(1, 2000, 50000, 0.1);
    EXPECT_FALSE(selectivity.maybe_always_true_can_ignore());
}

// Test that sampling_frequency survives VExprContext recreation, which is the
// exact scenario that caused the original bug.
TEST_F(VRuntimeFilterWrapperSamplingTest, sampling_frequency_survives_context_recreation) {
    auto stub = std::make_shared<StubVExpr>();
    auto node = TExprNodeBuilder(TExprNodeType::SLOT_REF,
                                 TTypeDescBuilder()
                                         .set_types(TTypeNodeBuilder()
                                                            .set_type(TTypeNodeType::SCALAR)
                                                            .set_scalar_type(TPrimitiveType::INT)
                                                            .build())
                                         .build(),
                                 0)
                        .build();

    const int expected_frequency = 32;
    auto wrapper = VRuntimeFilterWrapper::create_shared(node, stub, 0.4, false, /*filter_id=*/1,
                                                        expected_frequency);

    // First context - prepare and open work
    auto context1 = std::make_shared<VExprContext>(wrapper);
    RowDescriptor row_desc;
    ASSERT_TRUE(wrapper->prepare(_runtime_states[0].get(), row_desc, context1.get()).ok());
    ASSERT_TRUE(
            wrapper->open(_runtime_states[0].get(), context1.get(), FunctionContext::FRAGMENT_LOCAL)
                    .ok());

    // Create a brand new non-clone VExprContext with the same VRuntimeFilterWrapper,
    // matching the production path in _append_rf_into_conjuncts which calls
    // VExprContext::create_shared(expr) then conjunct->prepare() and conjunct->open().
    auto context2 = std::make_shared<VExprContext>(wrapper);
    EXPECT_FALSE(context2->get_runtime_filter_selectivity().maybe_always_true_can_ignore());

    // Drive the recreated context through prepare/open via VExprContext (not the
    // wrapper directly), matching the production _append_rf_into_conjuncts lifecycle.
    ASSERT_TRUE(context2->prepare(_runtime_states[0].get(), row_desc).ok());
    ASSERT_TRUE(context2->open(_runtime_states[0].get()).ok());

    // After open(), sampling_frequency should be propagated from VRuntimeFilterWrapper
    // to context2. Verify by accumulating low-selectivity data and checking that
    // always_true can be detected — this is the actual behavior the fix protects.
    auto& selectivity = context2->get_runtime_filter_selectivity();
    selectivity.update_judge_selectivity(1, 2000, 50000, 0.1);
    EXPECT_TRUE(selectivity.maybe_always_true_can_ignore());
}

} // namespace doris
