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
#include <gtest/gtest.h>

#include <limits>
#include <memory>

#include "pipeline/exec/mock_operator.h"
#include "pipeline/exec/mock_scan_operator.h"
#include "pipeline/operator/operator_helper.h"
#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_descriptors.h"
#include "testutil/mock/mock_fn_call.h"
#include "testutil/mock/mock_in_expr.h"
#include "testutil/mock/mock_literal_expr.h"
#include "testutil/mock/mock_slot_ref.h"
#include "vec/columns/column_const.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/functions/in.h"

namespace doris::pipeline {

using namespace vectorized;

struct ScanNormalizePredicate : public ::testing::Test {
    void SetUp() override {
        state = std::make_shared<MockRuntimeState>();
        op = std::make_shared<MockScanOperatorX>();
    }
    std::shared_ptr<MockRuntimeState> state;
    std::shared_ptr<MockScanOperatorX> op;
};

TEST_F(ScanNormalizePredicate, test1) {
    auto local_state = std::make_shared<MockScanLocalState>(state.get(), op.get());
    vectorized::VExprSPtr new_root;
    auto conjunct_expr_root =
            MockSlotRef::create_mock_context(0, std::make_shared<DataTypeInt64>());
    auto st = local_state->_normalize_predicate(conjunct_expr_root->root(),
                                                conjunct_expr_root.get(), new_root);
    EXPECT_TRUE(st) << st.msg();
    std::cout << new_root->debug_string() << std::endl;
}

TEST_F(ScanNormalizePredicate, test_eval_const_conjuncts1) {
    // case
    // Predicate false and xxx and xxx ....
    // In this case, it will not execute directly, wake up scan op, and set eos to true
    auto local_state = std::make_shared<MockScanLocalState>(state.get(), op.get());

    auto fn_eq = MockFnCall::create("eq");
    fn_eq->_mock_is_constant = true;
    fn_eq->set_const_expr_col(
            ColumnConst::create(ColumnHelper::create_column<DataTypeBool>({false}), 1));
    EXPECT_TRUE(fn_eq->is_constant());

    local_state->_scan_dependency = Dependency::create_shared(0, 0, "DEPENDENCY");

    EXPECT_FALSE(local_state->_scan_dependency->ready());
    EXPECT_FALSE(local_state->_eos);

    auto ctx = VExprContext::create_shared(fn_eq);
    ctx->_prepared = true;
    ctx->_opened = true;

    vectorized::VExprSPtr new_root;
    auto conjunct_expr_root = ctx;
    auto st = local_state->_normalize_predicate(conjunct_expr_root->root(),
                                                conjunct_expr_root.get(), new_root);

    EXPECT_EQ(new_root, nullptr);
    EXPECT_TRUE(local_state->_scan_dependency->ready());
    EXPECT_TRUE(local_state->_eos);
}

TEST_F(ScanNormalizePredicate, test_eval_const_conjuncts2) {
    // case
    // Predicate false and xxx and xxx ....
    // In this case, it will not execute directly, wake up scan op, and set eos to true
    auto local_state = std::make_shared<MockScanLocalState>(state.get(), op.get());

    auto fn_eq = MockFnCall::create("eq");
    fn_eq->_mock_is_constant = true;
    fn_eq->set_const_expr_col(ColumnHelper::create_column<DataTypeBool>({false}));
    EXPECT_TRUE(fn_eq->is_constant());

    local_state->_scan_dependency = Dependency::create_shared(0, 0, "DEPENDENCY");

    EXPECT_FALSE(local_state->_scan_dependency->ready());
    EXPECT_FALSE(local_state->_eos);

    auto ctx = VExprContext::create_shared(fn_eq);
    ctx->_prepared = true;
    ctx->_opened = true;

    vectorized::VExprSPtr new_root;
    auto conjunct_expr_root = ctx;
    auto st = local_state->_normalize_predicate(conjunct_expr_root->root(),
                                                conjunct_expr_root.get(), new_root);
    EXPECT_EQ(new_root, nullptr);
    EXPECT_TRUE(local_state->_scan_dependency->ready());
    EXPECT_TRUE(local_state->_eos);
}

TEST_F(ScanNormalizePredicate, test_eval_const_conjuncts3) {
    // case
    // Predicate false and xxx and xxx ....
    // The returned column is not a constant column, but the size is not equal to 1
    auto local_state = std::make_shared<MockScanLocalState>(state.get(), op.get());

    auto fn_eq = MockFnCall::create("eq");
    fn_eq->_mock_is_constant = true;
    fn_eq->set_const_expr_col(ColumnHelper::create_column<DataTypeBool>({false, false}));
    EXPECT_TRUE(fn_eq->is_constant());

    auto ctx = VExprContext::create_shared(fn_eq);
    ctx->_prepared = true;
    ctx->_opened = true;

    vectorized::VExprSPtr new_root;
    auto conjunct_expr_root = ctx;
    // There is a DCHECK in the code to ensure size must be equal to 1, wait for this part of the code to be removed later
    // auto st = local_state->_normalize_predicate(conjunct_expr_root->root(),
    //                                             conjunct_expr_root.get(), new_root);
    // EXPECT_FALSE(st.ok());
    // std::cout << st.msg() << std::endl;
}

TEST_F(ScanNormalizePredicate, test_eval_const_conjuncts4) {
    // case
    // Predicate false and xxx and xxx ....
    // The returned column is neither a constant column nor a boolean column
    auto local_state = std::make_shared<MockScanLocalState>(state.get(), op.get());

    auto fn_eq = MockFnCall::create("eq");
    fn_eq->_mock_is_constant = true;
    fn_eq->set_const_expr_col(ColumnHelper::create_column<DataTypeInt32>({false, false}));
    EXPECT_TRUE(fn_eq->is_constant());

    auto ctx = VExprContext::create_shared(fn_eq);
    ctx->_prepared = true;
    ctx->_opened = true;

    vectorized::VExprSPtr new_root;
    auto conjunct_expr_root = ctx;
    auto st = local_state->_normalize_predicate(conjunct_expr_root->root(),
                                                conjunct_expr_root.get(), new_root);
    EXPECT_TRUE(st.ok());
    std::cout << st.msg() << std::endl;
}

TEST_F(ScanNormalizePredicate, test_is_predicate_acting_on_slot1) {
    auto local_state = std::make_shared<MockScanLocalState>(state.get(), op.get());

    // case
    // slot eq 42
    // The final range is a fixed value 42

    const int SlotId = 0;

    SlotDescriptor slot_desc;
    slot_desc._type = vectorized::DataTypeFactory::instance().create_data_type(
            PrimitiveType::TYPE_BIGINT, false);

    ColumnValueRange<TYPE_BIGINT> range("mock", false, 0, 0);
    local_state->_slot_id_to_value_range[SlotId] = std::make_pair(&slot_desc, range);

    {
        auto slot_ref = std::make_shared<MockSlotRef>(0, std::make_shared<DataTypeInt64>());
        auto fn_eq = MockFnCall::create("eq");
        auto const_val = std::make_shared<MockLiteral>(
                ColumnHelper::create_column_with_name<DataTypeInt64>({42}));

        fn_eq->add_child(slot_ref);
        fn_eq->add_child(const_val);
        fn_eq->_node_type = TExprNodeType::BINARY_PRED;
        slot_ref->_slot_id = SlotId;
        EXPECT_FALSE(fn_eq->is_constant());

        auto ctx = VExprContext::create_shared(fn_eq);
        ctx->_prepared = true;
        ctx->_opened = true;

        vectorized::VExprSPtr new_root;
        auto conjunct_expr_root = ctx;
        auto st = local_state->_normalize_predicate(conjunct_expr_root->root(),
                                                    conjunct_expr_root.get(), new_root);
        EXPECT_TRUE(st.ok());
        std::cout << st.msg() << std::endl;
    }

    EXPECT_TRUE(local_state->_slot_id_to_value_range.contains(SlotId));

    auto& output_range = local_state->_slot_id_to_value_range[SlotId].second;

    std::visit(
            [](auto&& arg) {
                using T = std::decay_t<decltype(arg)>;
                if constexpr (std::is_same_v<T, ColumnValueRange<TYPE_BIGINT>>) {
                    EXPECT_EQ(arg._fixed_values.size(), 1);
                    auto it = arg._fixed_values.begin();
                    EXPECT_EQ(*it, 42);
                } else {
                    FAIL() << "unexpected type";
                }
            },
            output_range);
}

TEST_F(ScanNormalizePredicate, test_is_predicate_acting_on_slot2) {
    auto local_state = std::make_shared<MockScanLocalState>(state.get(), op.get());

    // case
    // slot eq 42 and slot eq 43
    // The two values conflict, and the final fixed value is empty
    const int SlotId = 0;

    SlotDescriptor slot_desc;
    slot_desc._type = vectorized::DataTypeFactory::instance().create_data_type(
            PrimitiveType::TYPE_BIGINT, false);

    ColumnValueRange<TYPE_BIGINT> range("mock", false, 0, 0);
    local_state->_slot_id_to_value_range[SlotId] = std::make_pair(&slot_desc, range);

    {
        auto slot_ref = std::make_shared<MockSlotRef>(0, std::make_shared<DataTypeInt64>());
        auto fn_eq = MockFnCall::create("eq");
        auto const_val = std::make_shared<MockLiteral>(
                ColumnHelper::create_column_with_name<DataTypeInt64>({42}));

        fn_eq->add_child(slot_ref);
        fn_eq->add_child(const_val);
        fn_eq->_node_type = TExprNodeType::BINARY_PRED;
        slot_ref->_slot_id = SlotId;
        EXPECT_FALSE(fn_eq->is_constant());

        auto ctx = VExprContext::create_shared(fn_eq);
        ctx->_prepared = true;
        ctx->_opened = true;

        vectorized::VExprSPtr new_root;
        auto conjunct_expr_root = ctx;
        auto st = local_state->_normalize_predicate(conjunct_expr_root->root(),
                                                    conjunct_expr_root.get(), new_root);
        EXPECT_TRUE(st.ok());
        std::cout << st.msg() << std::endl;
    }

    {
        auto slot_ref = std::make_shared<MockSlotRef>(0, std::make_shared<DataTypeInt64>());
        auto fn_eq = MockFnCall::create("eq");
        auto const_val = std::make_shared<MockLiteral>(
                ColumnHelper::create_column_with_name<DataTypeInt64>({43}));

        fn_eq->add_child(slot_ref);
        fn_eq->add_child(const_val);
        fn_eq->_node_type = TExprNodeType::BINARY_PRED;
        slot_ref->_slot_id = SlotId;
        EXPECT_FALSE(fn_eq->is_constant());

        auto ctx = VExprContext::create_shared(fn_eq);
        ctx->_prepared = true;
        ctx->_opened = true;

        vectorized::VExprSPtr new_root;
        auto conjunct_expr_root = ctx;
        auto st = local_state->_normalize_predicate(conjunct_expr_root->root(),
                                                    conjunct_expr_root.get(), new_root);
        EXPECT_TRUE(st.ok());
        std::cout << st.msg() << std::endl;
    }

    EXPECT_TRUE(local_state->_slot_id_to_value_range.contains(SlotId));

    auto& output_range = local_state->_slot_id_to_value_range[SlotId].second;

    std::visit(
            [](auto&& arg) {
                using T = std::decay_t<decltype(arg)>;
                if constexpr (std::is_same_v<T, ColumnValueRange<TYPE_BIGINT>>) {
                    EXPECT_EQ(arg._fixed_values.size(), 0);
                } else {
                    FAIL() << "unexpected type";
                }
            },
            output_range);
}

TEST_F(ScanNormalizePredicate, test_is_predicate_acting_on_slot3) {
    auto local_state = std::make_shared<MockScanLocalState>(state.get(), op.get());

    // case
    // slot eq null
    // 直接返回，设置eos = true

    const int SlotId = 0;

    SlotDescriptor slot_desc;
    slot_desc._type = vectorized::DataTypeFactory::instance().create_data_type(
            PrimitiveType::TYPE_BIGINT, false);

    ColumnValueRange<TYPE_BIGINT> range("mock", false, 0, 0);
    local_state->_slot_id_to_value_range[SlotId] = std::make_pair(&slot_desc, range);

    local_state->_scan_dependency = Dependency::create_shared(0, 0, "DEPENDENCY");

    EXPECT_FALSE(local_state->_scan_dependency->ready());
    EXPECT_FALSE(local_state->_eos);
    {
        auto slot_ref = std::make_shared<MockSlotRef>(0, std::make_shared<DataTypeInt64>());
        auto fn_eq = MockFnCall::create("eq");
        auto const_val = std::make_shared<MockLiteral>(
                ColumnHelper::create_nullable_column_with_name<DataTypeInt64>({42}, {true}));

        fn_eq->add_child(slot_ref);
        fn_eq->add_child(const_val);
        fn_eq->_node_type = TExprNodeType::BINARY_PRED;
        slot_ref->_slot_id = SlotId;
        EXPECT_FALSE(fn_eq->is_constant());

        auto ctx = VExprContext::create_shared(fn_eq);
        ctx->_prepared = true;
        ctx->_opened = true;

        vectorized::VExprSPtr new_root;
        auto conjunct_expr_root = ctx;
        EXPECT_TRUE(local_state->_normalize_predicate(conjunct_expr_root->root(),
                                                      conjunct_expr_root.get(), new_root));
    }
    EXPECT_TRUE(local_state->_scan_dependency->ready());
    EXPECT_TRUE(local_state->_eos);
}

TEST_F(ScanNormalizePredicate, test_is_predicate_acting_on_slot4) {
    auto local_state = std::make_shared<MockScanLocalState>(state.get(), op.get());

    // case
    //  slot in (1,10,100)

    const int SlotId = 0;

    SlotDescriptor slot_desc;
    slot_desc._type = vectorized::DataTypeFactory::instance().create_data_type(
            PrimitiveType::TYPE_BIGINT, false);

    ColumnValueRange<TYPE_BIGINT> range("mock", false, 0, 0);
    local_state->_slot_id_to_value_range[SlotId] = std::make_pair(&slot_desc, range);

    {
        auto slot_ref = std::make_shared<MockSlotRef>(0, std::make_shared<DataTypeInt64>());
        auto ctx = MockInExpr::create_with_ctx(
                ColumnHelper::create_column<DataTypeInt64>({1, 10, 100}));
        auto fn_in = ctx->root();

        fn_in->add_child(slot_ref);
        fn_in->_node_type = TExprNodeType::IN_PRED;
        slot_ref->_slot_id = SlotId;

        ctx->_prepared = true;
        ctx->_opened = true;

        vectorized::VExprSPtr new_root;
        auto conjunct_expr_root = ctx;
        EXPECT_TRUE(local_state->_normalize_predicate(conjunct_expr_root->root(),
                                                      conjunct_expr_root.get(), new_root));
    }

    EXPECT_TRUE(local_state->_slot_id_to_value_range.contains(SlotId));

    auto& output_range = local_state->_slot_id_to_value_range[SlotId].second;

    std::visit(
            [](auto&& arg) {
                using T = std::decay_t<decltype(arg)>;
                if constexpr (std::is_same_v<T, ColumnValueRange<TYPE_BIGINT>>) {
                    EXPECT_EQ(arg._fixed_values.size(), 3);
                    auto it = arg._fixed_values.begin();
                    EXPECT_EQ(*it, 1);
                    ++it;
                    EXPECT_EQ(*it, 10);
                    ++it;
                    EXPECT_EQ(*it, 100);
                } else {
                    FAIL() << "unexpected type";
                }
            },
            output_range);
}

TEST_F(ScanNormalizePredicate, test_is_predicate_acting_on_slot5) {
    auto local_state = std::make_shared<MockScanLocalState>(state.get(), op.get());

    // case
    //  slot in (1,10,100)

    const int SlotId = 0;

    SlotDescriptor slot_desc;
    slot_desc._type = vectorized::DataTypeFactory::instance().create_data_type(
            PrimitiveType::TYPE_BIGINT, false);

    ColumnValueRange<TYPE_BIGINT> range("mock", false, 0, 0);
    local_state->_slot_id_to_value_range[SlotId] = std::make_pair(&slot_desc, range);

    {
        auto slot_ref = std::make_shared<MockSlotRef>(0, std::make_shared<DataTypeInt64>());
        auto ctx = MockInExpr::create_with_ctx(
                ColumnHelper::create_column<DataTypeInt64>({1, 10, 100}));
        auto fn_in = ctx->root();

        fn_in->add_child(slot_ref);
        fn_in->_node_type = TExprNodeType::IN_PRED;
        slot_ref->_slot_id = SlotId;

        ctx->_prepared = true;
        ctx->_opened = true;

        vectorized::VExprSPtr new_root;
        auto conjunct_expr_root = ctx;
        EXPECT_TRUE(local_state->_normalize_predicate(conjunct_expr_root->root(),
                                                      conjunct_expr_root.get(), new_root));
    }

    EXPECT_TRUE(local_state->_slot_id_to_value_range.contains(SlotId));

    auto& output_range = local_state->_slot_id_to_value_range[SlotId].second;

    std::visit(
            [](auto&& arg) {
                using T = std::decay_t<decltype(arg)>;
                if constexpr (std::is_same_v<T, ColumnValueRange<TYPE_BIGINT>>) {
                    EXPECT_EQ(arg._fixed_values.size(), 3);
                    auto it = arg._fixed_values.begin();
                    EXPECT_EQ(*it, 1);
                    ++it;
                    EXPECT_EQ(*it, 10);
                    ++it;
                    EXPECT_EQ(*it, 100);
                } else {
                    FAIL() << "unexpected type";
                }
            },
            output_range);
}

TEST_F(ScanNormalizePredicate, test_is_predicate_acting_on_slot6) {
    auto local_state = std::make_shared<MockScanLocalState>(state.get(), op.get());

    // case
    //  slot in (col,1,10,100)

    const int SlotId = 0;

    SlotDescriptor slot_desc;
    slot_desc._type = vectorized::DataTypeFactory::instance().create_data_type(
            PrimitiveType::TYPE_BIGINT, false);

    ColumnValueRange<TYPE_BIGINT> range("mock", false, 0, 0);
    local_state->_slot_id_to_value_range[SlotId] = std::make_pair(&slot_desc, range);

    {
        auto slot_ref = std::make_shared<MockSlotRef>(0, std::make_shared<DataTypeInt64>());
        auto ctx = MockInExpr::create_with_ctx(
                ColumnHelper::create_column<DataTypeInt64>({1, 10, 100}));
        auto fn_in = ctx->root();

        fn_in->add_child(slot_ref);
        fn_in->_node_type = TExprNodeType::IN_PRED;
        slot_ref->_slot_id = SlotId;

        ctx->_prepared = true;
        ctx->_opened = true;

        auto* state = reinterpret_cast<vectorized::InState*>(
                ctx->fn_context(fn_in->fn_context_index())
                        ->get_function_state(FunctionContext::FRAGMENT_LOCAL));

        state->use_set = false;

        vectorized::VExprSPtr new_root;
        auto conjunct_expr_root = ctx;
        EXPECT_TRUE(local_state->_normalize_predicate(conjunct_expr_root->root(),
                                                      conjunct_expr_root.get(), new_root));
    }

    EXPECT_TRUE(local_state->_slot_id_to_value_range.contains(SlotId));

    auto& output_range = local_state->_slot_id_to_value_range[SlotId].second;

    std::visit(
            [](auto&& arg) {
                using T = std::decay_t<decltype(arg)>;
                if constexpr (std::is_same_v<T, ColumnValueRange<TYPE_BIGINT>>) {
                    EXPECT_EQ(arg._fixed_values.size(), 0);
                } else {
                    FAIL() << "unexpected type";
                }
            },
            output_range);
}

TEST_F(ScanNormalizePredicate, test_is_predicate_acting_on_slot7) {
    auto local_state = std::make_shared<MockScanLocalState>(state.get(), op.get());

    // case
    //  slot not in (null,1,10,100)

    const int SlotId = 0;

    SlotDescriptor slot_desc;
    slot_desc._type = vectorized::DataTypeFactory::instance().create_data_type(
            PrimitiveType::TYPE_BIGINT, false);

    ColumnValueRange<TYPE_BIGINT> range("mock", false, 0, 0);
    local_state->_slot_id_to_value_range[SlotId] = std::make_pair(&slot_desc, range);

    {
        auto slot_ref = std::make_shared<MockSlotRef>(0, std::make_shared<DataTypeInt64>());
        auto ctx = MockInExpr::create_with_ctx(
                ColumnHelper::create_column<DataTypeInt64>({1, 10, 100}), true);
        auto fn_in = ctx->root();

        fn_in->add_child(slot_ref);
        fn_in->_node_type = TExprNodeType::IN_PRED;
        slot_ref->_slot_id = SlotId;

        ctx->_prepared = true;
        ctx->_opened = true;

        auto* state = reinterpret_cast<vectorized::InState*>(
                ctx->fn_context(fn_in->fn_context_index())
                        ->get_function_state(FunctionContext::FRAGMENT_LOCAL));

        state->use_set = true;
        state->hybrid_set->insert(nullptr, 0);

        local_state->_scan_dependency = Dependency::create_shared(0, 0, "DEPENDENCY");

        EXPECT_FALSE(local_state->_scan_dependency->ready());
        EXPECT_FALSE(local_state->_eos);

        vectorized::VExprSPtr new_root;
        auto conjunct_expr_root = ctx;
        EXPECT_TRUE(local_state->_normalize_predicate(conjunct_expr_root->root(),
                                                      conjunct_expr_root.get(), new_root));
    }

    EXPECT_TRUE(local_state->_scan_dependency->ready());
    EXPECT_TRUE(local_state->_eos);
}

TEST_F(ScanNormalizePredicate, test_is_predicate_acting_on_slot8) {
    auto local_state = std::make_shared<MockScanLocalState>(state.get(), op.get());

    // case
    // range (1,10,100,1000)
    //  slot not in (1,10,100)

    const int SlotId = 0;

    SlotDescriptor slot_desc;
    slot_desc._type = vectorized::DataTypeFactory::instance().create_data_type(
            PrimitiveType::TYPE_BIGINT, false);

    ColumnValueRange<TYPE_BIGINT> range("mock", false, 0, 0);
    EXPECT_TRUE(range.add_fixed_value(1));
    EXPECT_TRUE(range.add_fixed_value(10));
    EXPECT_TRUE(range.add_fixed_value(100));
    EXPECT_TRUE(range.add_fixed_value(1000));

    local_state->_slot_id_to_value_range[SlotId] = std::make_pair(&slot_desc, range);

    {
        auto slot_ref = std::make_shared<MockSlotRef>(0, std::make_shared<DataTypeInt64>());
        auto ctx = MockInExpr::create_with_ctx(
                ColumnHelper::create_column<DataTypeInt64>({1, 10, 100}), true);
        auto fn_in = ctx->root();

        fn_in->add_child(slot_ref);
        fn_in->_node_type = TExprNodeType::IN_PRED;
        slot_ref->_slot_id = SlotId;

        ctx->_prepared = true;
        ctx->_opened = true;

        vectorized::VExprSPtr new_root;
        auto conjunct_expr_root = ctx;
        EXPECT_TRUE(local_state->_normalize_predicate(conjunct_expr_root->root(),
                                                      conjunct_expr_root.get(), new_root));
    }

    auto& output_range = local_state->_slot_id_to_value_range[SlotId].second;
    std::visit(
            [](auto&& arg) {
                using T = std::decay_t<decltype(arg)>;
                if constexpr (std::is_same_v<T, ColumnValueRange<TYPE_BIGINT>>) {
                    EXPECT_EQ(arg._fixed_values.size(), 1);
                    auto it = arg._fixed_values.begin();
                    EXPECT_EQ(*it, 1000);
                } else {
                    FAIL() << "unexpected type";
                }
            },
            output_range);
}

TEST_F(ScanNormalizePredicate, test_is_predicate_acting_on_slot10) {
    auto local_state = std::make_shared<MockScanLocalState>(state.get(), op.get());

    // case
    // range ()
    //  slot not in (1,10,100)

    const int SlotId = 0;

    SlotDescriptor slot_desc;
    slot_desc._type = vectorized::DataTypeFactory::instance().create_data_type(
            PrimitiveType::TYPE_BIGINT, false);

    ColumnValueRange<TYPE_BIGINT> range("mock", false, 0, 0);

    local_state->_slot_id_to_value_range[SlotId] = std::make_pair(&slot_desc, range);

    {
        auto slot_ref = std::make_shared<MockSlotRef>(0, std::make_shared<DataTypeInt64>());
        auto ctx = MockInExpr::create_with_ctx(
                ColumnHelper::create_column<DataTypeInt64>({1, 10, 100}), true);
        auto fn_in = ctx->root();

        fn_in->add_child(slot_ref);
        fn_in->_node_type = TExprNodeType::IN_PRED;
        slot_ref->_slot_id = SlotId;

        ctx->_prepared = true;
        ctx->_opened = true;

        vectorized::VExprSPtr new_root;
        auto conjunct_expr_root = ctx;
        EXPECT_TRUE(local_state->_normalize_predicate(conjunct_expr_root->root(),
                                                      conjunct_expr_root.get(), new_root));
    }

    auto& output_range = local_state->_not_in_value_ranges.front();
    std::visit(
            [](auto&& arg) {
                using T = std::decay_t<decltype(arg)>;
                if constexpr (std::is_same_v<T, ColumnValueRange<TYPE_BIGINT>>) {
                    EXPECT_EQ(arg._fixed_values.size(), 3);
                    auto it = arg._fixed_values.begin();
                    EXPECT_EQ(*it, 1);
                    ++it;
                    EXPECT_EQ(*it, 10);
                    ++it;
                    EXPECT_EQ(*it, 100);
                } else {
                    FAIL() << "unexpected type";
                }
            },
            output_range);
}

TEST_F(ScanNormalizePredicate, test_is_predicate_acting_on_slot11) {
    auto local_state = std::make_shared<MockScanLocalState>(state.get(), op.get());

    // case
    // range ()
    //  slot not eq 100

    const int SlotId = 0;

    SlotDescriptor slot_desc;
    slot_desc._type = vectorized::DataTypeFactory::instance().create_data_type(
            PrimitiveType::TYPE_BIGINT, false);

    ColumnValueRange<TYPE_BIGINT> range("mock", false, 0, 0);

    local_state->_slot_id_to_value_range[SlotId] = std::make_pair(&slot_desc, range);

    {
        auto slot_ref = std::make_shared<MockSlotRef>(0, std::make_shared<DataTypeInt64>());
        auto fn_eq = MockFnCall::create("ne");
        auto const_val = std::make_shared<MockLiteral>(
                ColumnHelper::create_column_with_name<DataTypeInt64>({100}));

        fn_eq->add_child(slot_ref);
        fn_eq->add_child(const_val);
        fn_eq->_node_type = TExprNodeType::BINARY_PRED;
        slot_ref->_slot_id = SlotId;
        EXPECT_FALSE(fn_eq->is_constant());

        auto ctx = VExprContext::create_shared(fn_eq);
        ctx->_prepared = true;
        ctx->_opened = true;

        vectorized::VExprSPtr new_root;
        auto conjunct_expr_root = ctx;
        EXPECT_TRUE(local_state->_normalize_predicate(conjunct_expr_root->root(),
                                                      conjunct_expr_root.get(), new_root));
    }

    auto& output_range = local_state->_not_in_value_ranges.front();
    std::visit(
            [](auto&& arg) {
                using T = std::decay_t<decltype(arg)>;
                if constexpr (std::is_same_v<T, ColumnValueRange<TYPE_BIGINT>>) {
                    EXPECT_EQ(arg._fixed_values.size(), 1);
                    auto it = arg._fixed_values.begin();
                    EXPECT_EQ(*it, 100);
                } else {
                    FAIL() << "unexpected type";
                }
            },
            output_range);
}

TEST_F(ScanNormalizePredicate, test_is_predicate_acting_on_slot12) {
    auto local_state = std::make_shared<MockScanLocalState>(state.get(), op.get());

    // case
    // range (1，10，100)
    //  slot is null

    const int SlotId = 0;

    SlotDescriptor slot_desc;
    slot_desc._type = vectorized::DataTypeFactory::instance().create_data_type(
            PrimitiveType::TYPE_BIGINT, false);

    ColumnValueRange<TYPE_BIGINT> range("mock", false, 0, 0);
    EXPECT_TRUE(range.add_fixed_value(1));
    EXPECT_TRUE(range.add_fixed_value(10));
    EXPECT_TRUE(range.add_fixed_value(100));

    local_state->_slot_id_to_value_range[SlotId] = std::make_pair(&slot_desc, range);

    {
        auto slot_ref = std::make_shared<MockSlotRef>(0, std::make_shared<DataTypeInt64>());
        auto fn_eq = MockFnCall::create("is_null_pred");
        auto const_val = std::make_shared<MockLiteral>(
                ColumnHelper::create_column_with_name<DataTypeInt64>({100}));

        fn_eq->add_child(slot_ref);
        fn_eq->add_child(const_val);
        fn_eq->_node_type = TExprNodeType::FUNCTION_CALL;
        slot_ref->_slot_id = SlotId;
        EXPECT_FALSE(fn_eq->is_constant());

        auto ctx = VExprContext::create_shared(fn_eq);
        ctx->_prepared = true;
        ctx->_opened = true;

        vectorized::VExprSPtr new_root;
        auto conjunct_expr_root = ctx;
        EXPECT_TRUE(local_state->_normalize_predicate(conjunct_expr_root->root(),
                                                      conjunct_expr_root.get(), new_root));
    }

    auto& output_range = local_state->_slot_id_to_value_range[SlotId].second;
    std::visit(
            [](auto&& arg) {
                using T = std::decay_t<decltype(arg)>;
                if constexpr (std::is_same_v<T, ColumnValueRange<TYPE_BIGINT>>) {
                    EXPECT_EQ(arg._fixed_values.size(), 0);
                } else {
                    FAIL() << "unexpected type";
                }
            },
            output_range);
}

TEST_F(ScanNormalizePredicate, test_is_predicate_acting_on_slot13) {
    auto local_state = std::make_shared<MockScanLocalState>(state.get(), op.get());

    // case
    // range (1，10，100)
    //  slot is not null

    const int SlotId = 0;

    SlotDescriptor slot_desc;
    slot_desc._type = vectorized::DataTypeFactory::instance().create_data_type(
            PrimitiveType::TYPE_BIGINT, false);

    ColumnValueRange<TYPE_BIGINT> range("mock", false, 0, 0);
    EXPECT_TRUE(range.add_fixed_value(1));
    EXPECT_TRUE(range.add_fixed_value(10));
    EXPECT_TRUE(range.add_fixed_value(100));

    local_state->_slot_id_to_value_range[SlotId] = std::make_pair(&slot_desc, range);

    {
        auto slot_ref = std::make_shared<MockSlotRef>(0, std::make_shared<DataTypeInt64>());
        auto fn_eq = MockFnCall::create("is_not_null_pred");
        auto const_val = std::make_shared<MockLiteral>(
                ColumnHelper::create_column_with_name<DataTypeInt64>({100}));

        fn_eq->add_child(slot_ref);
        fn_eq->add_child(const_val);
        fn_eq->_node_type = TExprNodeType::FUNCTION_CALL;
        slot_ref->_slot_id = SlotId;
        EXPECT_FALSE(fn_eq->is_constant());

        auto ctx = VExprContext::create_shared(fn_eq);
        ctx->_prepared = true;
        ctx->_opened = true;

        vectorized::VExprSPtr new_root;
        auto conjunct_expr_root = ctx;
        EXPECT_TRUE(local_state->_normalize_predicate(conjunct_expr_root->root(),
                                                      conjunct_expr_root.get(), new_root));
    }

    auto& output_range = local_state->_slot_id_to_value_range[SlotId].second;
    std::visit(
            [](auto&& arg) {
                using T = std::decay_t<decltype(arg)>;
                if constexpr (std::is_same_v<T, ColumnValueRange<TYPE_BIGINT>>) {
                    EXPECT_EQ(arg._fixed_values.size(), 3);
                } else {
                    FAIL() << "unexpected type";
                }
            },
            output_range);
}

TEST_F(ScanNormalizePredicate, test_is_predicate_acting_on_slot14) {
    auto local_state = std::make_shared<MockScanLocalState>(state.get(), op.get());

    // case
    // range (1，10，100)
    //  slot  <= 10

    const int SlotId = 0;

    SlotDescriptor slot_desc;
    slot_desc._type = vectorized::DataTypeFactory::instance().create_data_type(
            PrimitiveType::TYPE_BIGINT, false);

    ColumnValueRange<TYPE_BIGINT> range("mock", false, 0, 0);
    EXPECT_TRUE(range.add_fixed_value(1));
    EXPECT_TRUE(range.add_fixed_value(10));
    EXPECT_TRUE(range.add_fixed_value(100));

    local_state->_slot_id_to_value_range[SlotId] = std::make_pair(&slot_desc, range);

    {
        auto slot_ref = std::make_shared<MockSlotRef>(0, std::make_shared<DataTypeInt64>());
        auto fn_eq = MockFnCall::create("le");
        auto const_val = std::make_shared<MockLiteral>(
                ColumnHelper::create_column_with_name<DataTypeInt64>({10}));

        fn_eq->add_child(slot_ref);
        fn_eq->add_child(const_val);
        fn_eq->_node_type = TExprNodeType::BINARY_PRED;
        slot_ref->_slot_id = SlotId;
        EXPECT_FALSE(fn_eq->is_constant());

        auto ctx = VExprContext::create_shared(fn_eq);
        ctx->_prepared = true;
        ctx->_opened = true;

        vectorized::VExprSPtr new_root;
        auto conjunct_expr_root = ctx;
        EXPECT_TRUE(local_state->_normalize_predicate(conjunct_expr_root->root(),
                                                      conjunct_expr_root.get(), new_root));
    }

    auto& output_range = local_state->_slot_id_to_value_range[SlotId].second;
    std::visit(
            [](auto&& arg) {
                using T = std::decay_t<decltype(arg)>;
                if constexpr (std::is_same_v<T, ColumnValueRange<TYPE_BIGINT>>) {
                    EXPECT_EQ(arg._fixed_values.size(), 2);
                    auto it = arg._fixed_values.begin();
                    EXPECT_EQ(*it, 1);
                    ++it;
                    EXPECT_EQ(*it, 10);
                } else {
                    FAIL() << "unexpected type";
                }
            },
            output_range);
}

TEST_F(ScanNormalizePredicate, test_is_predicate_acting_on_slot15) {
    auto local_state = std::make_shared<MockScanLocalState>(state.get(), op.get());

    // case
    // range (1，10，100)
    //  slot  <= 10

    const int SlotId = 0;

    SlotDescriptor slot_desc;
    slot_desc._type = vectorized::DataTypeFactory::instance().create_data_type(
            PrimitiveType::TYPE_BIGINT, false);

    ColumnValueRange<TYPE_BIGINT> range("mock", false, 0, 0);
    EXPECT_TRUE(range.add_fixed_value(1));
    EXPECT_TRUE(range.add_fixed_value(10));
    EXPECT_TRUE(range.add_fixed_value(100));

    local_state->_slot_id_to_value_range[SlotId] = std::make_pair(&slot_desc, range);

    {
        auto slot_ref = std::make_shared<MockSlotRef>(0, std::make_shared<DataTypeInt64>());
        auto fn_eq = MockFnCall::create("ge");
        auto const_val = std::make_shared<MockLiteral>(
                ColumnHelper::create_column_with_name<DataTypeInt64>({10}));

        fn_eq->add_child(slot_ref);
        fn_eq->add_child(const_val);
        fn_eq->_node_type = TExprNodeType::BINARY_PRED;
        slot_ref->_slot_id = SlotId;
        EXPECT_FALSE(fn_eq->is_constant());

        auto ctx = VExprContext::create_shared(fn_eq);
        ctx->_prepared = true;
        ctx->_opened = true;

        vectorized::VExprSPtr new_root;
        auto conjunct_expr_root = ctx;
        EXPECT_TRUE(local_state->_normalize_predicate(conjunct_expr_root->root(),
                                                      conjunct_expr_root.get(), new_root));
    }

    auto& output_range = local_state->_slot_id_to_value_range[SlotId].second;
    std::visit(
            [](auto&& arg) {
                using T = std::decay_t<decltype(arg)>;
                if constexpr (std::is_same_v<T, ColumnValueRange<TYPE_BIGINT>>) {
                    EXPECT_EQ(arg._fixed_values.size(), 2);
                    auto it = arg._fixed_values.begin();
                    EXPECT_EQ(*it, 10);
                    ++it;
                    EXPECT_EQ(*it, 100);
                } else {
                    FAIL() << "unexpected type";
                }
            },
            output_range);
}

TEST_F(ScanNormalizePredicate, test_double_predicate) {
    std::vector<double> test_values = {std::numeric_limits<double>::quiet_NaN(),
                                       std::numeric_limits<double>::infinity(),
                                       -std::numeric_limits<double>::infinity(),
                                       std::numeric_limits<double>::lowest(),
                                       std::numeric_limits<double>::max(),
                                       std::numeric_limits<float>::lowest(),
                                       std::numeric_limits<float>::max(),
                                       123456.789012345,
                                       -123456.789012345,
                                       0.0};
    const int SlotId = 0;

    SlotDescriptor slot_desc;
    slot_desc._type = vectorized::DataTypeFactory::instance().create_data_type(
            PrimitiveType::TYPE_DOUBLE, false);
    SlotDescriptor nullable_slot_desc;
    nullable_slot_desc._type = vectorized::DataTypeFactory::instance().create_data_type(
            PrimitiveType::TYPE_DOUBLE, true);
    // test eq
    for (auto const_v : test_values) {
        auto local_state = std::make_shared<MockScanLocalState>(state.get(), op.get());
        ColumnValueRange<TYPE_DOUBLE> range("mock", false, 0, 0);
        local_state->_slot_id_to_value_range[SlotId] = std::make_pair(&slot_desc, range);

        auto slot_ref = std::make_shared<MockSlotRef>(0, std::make_shared<DataTypeFloat64>());
        auto fn_eq = MockFnCall::create("eq");
        auto const_val = std::make_shared<MockLiteral>(
                ColumnHelper::create_column_with_name<DataTypeFloat64>({const_v}));

        fn_eq->add_child(slot_ref);
        fn_eq->add_child(const_val);
        fn_eq->_node_type = TExprNodeType::BINARY_PRED;
        slot_ref->_slot_id = SlotId;
        EXPECT_FALSE(fn_eq->is_constant());

        auto ctx = VExprContext::create_shared(fn_eq);
        ctx->_prepared = true;
        ctx->_opened = true;

        vectorized::VExprSPtr new_root;
        auto conjunct_expr_root = ctx;
        auto st = local_state->_normalize_predicate(conjunct_expr_root->root(),
                                                    conjunct_expr_root.get(), new_root);
        EXPECT_TRUE(st.ok());
        EXPECT_EQ(new_root, nullptr);

        EXPECT_TRUE(local_state->_slot_id_to_value_range.contains(SlotId));
        const auto& output_range = local_state->_slot_id_to_value_range[SlotId].second;
        std::visit(
                [&](auto&& arg) {
                    using T = std::decay_t<decltype(arg)>;
                    if constexpr (std::is_same_v<T, ColumnValueRange<TYPE_DOUBLE>>) {
                        EXPECT_EQ(arg._fixed_values.size(), 1);
                        auto it = arg._fixed_values.begin();
                        EXPECT_TRUE(Compare::equal(*it, const_v));
                    } else {
                        FAIL() << "unexpected type";
                    }
                },
                output_range);
    }
    // test in
    {
        auto local_state = std::make_shared<MockScanLocalState>(state.get(), op.get());
        ColumnValueRange<TYPE_DOUBLE> range("mock", false, 0, 0);
        local_state->_slot_id_to_value_range[SlotId] = std::make_pair(&slot_desc, range);
        auto slot_ref = std::make_shared<MockSlotRef>(0, std::make_shared<DataTypeFloat64>());
        auto ctx = MockInExpr::create_with_ctx(
                ColumnHelper::create_column<DataTypeFloat64>(test_values));
        auto fn_in = ctx->root();

        fn_in->add_child(slot_ref);
        fn_in->_node_type = TExprNodeType::IN_PRED;
        slot_ref->_slot_id = SlotId;

        ctx->_prepared = true;
        ctx->_opened = true;

        vectorized::VExprSPtr new_root;
        auto conjunct_expr_root = ctx;
        auto st = local_state->_normalize_predicate(conjunct_expr_root->root(),
                                                    conjunct_expr_root.get(), new_root);
        EXPECT_TRUE(st.ok());
        EXPECT_EQ(new_root, nullptr);
        EXPECT_TRUE(local_state->_slot_id_to_value_range.contains(SlotId));

        auto& output_range = local_state->_slot_id_to_value_range[SlotId].second;
        std::visit(
                [&](auto&& arg) {
                    using T = std::decay_t<decltype(arg)>;
                    if constexpr (std::is_same_v<T, ColumnValueRange<TYPE_DOUBLE>>) {
                        EXPECT_EQ(arg._fixed_values.size(), test_values.size());
                    } else {
                        FAIL() << "unexpected type";
                    }
                },
                output_range);
    }
    // test ne
    for (auto const_v : test_values) {
        auto local_state = std::make_shared<MockScanLocalState>(state.get(), op.get());
        ColumnValueRange<TYPE_DOUBLE> range("mock", false, 0, 0);
        local_state->_slot_id_to_value_range[SlotId] = std::make_pair(&slot_desc, range);
        auto slot_ref = std::make_shared<MockSlotRef>(0, std::make_shared<DataTypeFloat64>());
        auto fn_eq = MockFnCall::create("ne");
        auto const_val = std::make_shared<MockLiteral>(
                ColumnHelper::create_column_with_name<DataTypeFloat64>({const_v}));

        fn_eq->add_child(slot_ref);
        fn_eq->add_child(const_val);
        fn_eq->_node_type = TExprNodeType::BINARY_PRED;
        slot_ref->_slot_id = SlotId;
        EXPECT_FALSE(fn_eq->is_constant());

        auto ctx = VExprContext::create_shared(fn_eq);
        ctx->_prepared = true;
        ctx->_opened = true;

        vectorized::VExprSPtr new_root;
        auto conjunct_expr_root = ctx;
        EXPECT_TRUE(local_state->_normalize_predicate(conjunct_expr_root->root(),
                                                      conjunct_expr_root.get(), new_root));
        EXPECT_EQ(new_root, nullptr);
        EXPECT_TRUE(local_state->_slot_id_to_value_range.contains(SlotId));

        auto& output_range = local_state->_not_in_value_ranges.front();
        std::visit(
                [&](auto&& arg) {
                    using T = std::decay_t<decltype(arg)>;
                    if constexpr (std::is_same_v<T, ColumnValueRange<TYPE_DOUBLE>>) {
                        EXPECT_EQ(arg._fixed_values.size(), 1);
                        auto it = arg._fixed_values.begin();
                        EXPECT_TRUE(Compare::equal(*it, const_v));
                    } else {
                        FAIL() << "unexpected type";
                    }
                },
                output_range);
    }
    // test not in
    {
        auto local_state = std::make_shared<MockScanLocalState>(state.get(), op.get());
        ColumnValueRange<TYPE_DOUBLE> range("mock", false, 0, 0);
        local_state->_slot_id_to_value_range[SlotId] = std::make_pair(&slot_desc, range);
        auto slot_ref = std::make_shared<MockSlotRef>(0, std::make_shared<DataTypeFloat64>());
        auto ctx = MockInExpr::create_with_ctx(
                ColumnHelper::create_column<DataTypeFloat64>(test_values), true);
        auto fn_in = ctx->root();

        fn_in->add_child(slot_ref);
        fn_in->_node_type = TExprNodeType::IN_PRED;
        slot_ref->_slot_id = SlotId;

        ctx->_prepared = true;
        ctx->_opened = true;

        vectorized::VExprSPtr new_root;
        auto conjunct_expr_root = ctx;
        auto st = local_state->_normalize_predicate(conjunct_expr_root->root(),
                                                    conjunct_expr_root.get(), new_root);
        EXPECT_TRUE(st.ok());
        EXPECT_EQ(new_root, nullptr);
        EXPECT_TRUE(local_state->_slot_id_to_value_range.contains(SlotId));

        auto& output_range = local_state->_not_in_value_ranges.front();
        std::visit(
                [&](auto&& arg) {
                    using T = std::decay_t<decltype(arg)>;
                    if constexpr (std::is_same_v<T, ColumnValueRange<TYPE_DOUBLE>>) {
                        EXPECT_EQ(arg._fixed_values.size(), test_values.size());
                    } else {
                        FAIL() << "unexpected type";
                    }
                },
                output_range);
    }
    // test is null
    {
        auto local_state = std::make_shared<MockScanLocalState>(state.get(), op.get());
        ColumnValueRange<TYPE_DOUBLE> range("mock", true, 0, 0);
        local_state->_slot_id_to_value_range[SlotId] = std::make_pair(&nullable_slot_desc, range);
        auto slot_ref = std::make_shared<MockSlotRef>(
                0, std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>()));
        auto fn_eq = MockFnCall::create("is_null_pred");

        fn_eq->add_child(slot_ref);
        fn_eq->_node_type = TExprNodeType::FUNCTION_CALL;
        slot_ref->_slot_id = SlotId;
        EXPECT_FALSE(fn_eq->is_constant());

        auto ctx = VExprContext::create_shared(fn_eq);
        ctx->_prepared = true;
        ctx->_opened = true;

        vectorized::VExprSPtr new_root;
        auto conjunct_expr_root = ctx;
        EXPECT_TRUE(local_state->_normalize_predicate(conjunct_expr_root->root(),
                                                      conjunct_expr_root.get(), new_root));
        auto& output_range = local_state->_slot_id_to_value_range[SlotId].second;
        std::visit(
                [](auto&& arg) {
                    using T = std::decay_t<decltype(arg)>;
                    if constexpr (std::is_same_v<T, ColumnValueRange<TYPE_DOUBLE>>) {
                        EXPECT_EQ(arg._fixed_values.size(), 0);
                        EXPECT_FALSE(arg.is_fixed_value_range());
                        EXPECT_TRUE(arg.contain_null());
                    } else {
                        FAIL() << "unexpected type";
                    }
                },
                output_range);
    }
    // test is not null
    {
        auto local_state = std::make_shared<MockScanLocalState>(state.get(), op.get());
        ColumnValueRange<TYPE_DOUBLE> range("mock", true, 0, 0);
        local_state->_slot_id_to_value_range[SlotId] = std::make_pair(&nullable_slot_desc, range);
        auto slot_ref = std::make_shared<MockSlotRef>(
                0, std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>()));
        auto fn_eq = MockFnCall::create("is_not_null_pred");

        fn_eq->add_child(slot_ref);
        fn_eq->_node_type = TExprNodeType::FUNCTION_CALL;
        slot_ref->_slot_id = SlotId;
        EXPECT_FALSE(fn_eq->is_constant());

        auto ctx = VExprContext::create_shared(fn_eq);
        ctx->_prepared = true;
        ctx->_opened = true;

        vectorized::VExprSPtr new_root;
        auto conjunct_expr_root = ctx;
        EXPECT_TRUE(local_state->_normalize_predicate(conjunct_expr_root->root(),
                                                      conjunct_expr_root.get(), new_root));
        auto& output_range = local_state->_slot_id_to_value_range[SlotId].second;
        std::visit(
                [](auto&& arg) {
                    using T = std::decay_t<decltype(arg)>;
                    if constexpr (std::is_same_v<T, ColumnValueRange<TYPE_DOUBLE>>) {
                        EXPECT_FALSE(arg.is_fixed_value_range());
                        EXPECT_FALSE(arg.contain_null());
                    } else {
                        FAIL() << "unexpected type";
                    }
                },
                output_range);
    }
    // test less
    for (auto const_v : test_values) {
        // std::cout << "test less const_v=" << const_v << std::endl;
        auto local_state = std::make_shared<MockScanLocalState>(state.get(), op.get());
        ColumnValueRange<TYPE_DOUBLE> range("mock", false, 0, 0);
        local_state->_slot_id_to_value_range[SlotId] = std::make_pair(&slot_desc, range);

        auto slot_ref = std::make_shared<MockSlotRef>(0, std::make_shared<DataTypeFloat64>());
        auto fn_eq = MockFnCall::create("lt");
        auto const_val = std::make_shared<MockLiteral>(
                ColumnHelper::create_column_with_name<DataTypeFloat64>({const_v}));

        fn_eq->add_child(slot_ref);
        fn_eq->add_child(const_val);
        fn_eq->_node_type = TExprNodeType::BINARY_PRED;
        slot_ref->_slot_id = SlotId;
        EXPECT_FALSE(fn_eq->is_constant());

        auto ctx = VExprContext::create_shared(fn_eq);
        ctx->_prepared = true;
        ctx->_opened = true;

        vectorized::VExprSPtr new_root;
        auto conjunct_expr_root = ctx;
        auto st = local_state->_normalize_predicate(conjunct_expr_root->root(),
                                                    conjunct_expr_root.get(), new_root);
        EXPECT_TRUE(st.ok());
        EXPECT_EQ(new_root, nullptr);

        EXPECT_TRUE(local_state->_slot_id_to_value_range.contains(SlotId));
        const auto& output_range = local_state->_slot_id_to_value_range[SlotId].second;
        /*
        _low_value = -inf,
        _high_value = 90,
        _low_op = doris::FILTER_LARGER_OR_EQUAL,
        _high_op = doris::FILTER_LESS,
        */
        std::visit(
                [&](auto&& arg) {
                    using T = std::decay_t<decltype(arg)>;
                    if constexpr (std::is_same_v<T, ColumnValueRange<TYPE_DOUBLE>>) {
                        EXPECT_FALSE(arg.is_fixed_value_range());
                        if (const_v == -std::numeric_limits<double>::infinity()) {
                            EXPECT_TRUE(arg.is_empty_value_range());
                        } else {
                            EXPECT_TRUE(arg.is_scope_value_range());
                            EXPECT_EQ(arg.get_range_low_op(), doris::FILTER_LARGER_OR_EQUAL);
                            EXPECT_EQ(arg.get_range_high_op(), doris::FILTER_LESS);
                            EXPECT_TRUE(arg.is_low_value_minimum());
                            EXPECT_TRUE(Compare::equal(arg.get_range_max_value(), const_v));
                        }
                    } else {
                        FAIL() << "unexpected type";
                    }
                },
                output_range);
    }
    // test less or equal
    for (auto const_v : test_values) {
        // std::cout << "test less or equal const_v=" << const_v << std::endl;
        auto local_state = std::make_shared<MockScanLocalState>(state.get(), op.get());
        ColumnValueRange<TYPE_DOUBLE> range("mock", false, 0, 0);
        local_state->_slot_id_to_value_range[SlotId] = std::make_pair(&slot_desc, range);

        auto slot_ref = std::make_shared<MockSlotRef>(0, std::make_shared<DataTypeFloat64>());
        auto fn_eq = MockFnCall::create("le");
        auto const_val = std::make_shared<MockLiteral>(
                ColumnHelper::create_column_with_name<DataTypeFloat64>({const_v}));

        fn_eq->add_child(slot_ref);
        fn_eq->add_child(const_val);
        fn_eq->_node_type = TExprNodeType::BINARY_PRED;
        slot_ref->_slot_id = SlotId;
        EXPECT_FALSE(fn_eq->is_constant());

        auto ctx = VExprContext::create_shared(fn_eq);
        ctx->_prepared = true;
        ctx->_opened = true;

        vectorized::VExprSPtr new_root;
        auto conjunct_expr_root = ctx;
        auto st = local_state->_normalize_predicate(conjunct_expr_root->root(),
                                                    conjunct_expr_root.get(), new_root);
        EXPECT_TRUE(st.ok());
        EXPECT_EQ(new_root, nullptr);

        EXPECT_TRUE(local_state->_slot_id_to_value_range.contains(SlotId));
        const auto& output_range = local_state->_slot_id_to_value_range[SlotId].second;
        std::visit(
                [&](auto&& arg) {
                    using T = std::decay_t<decltype(arg)>;
                    if constexpr (std::is_same_v<T, ColumnValueRange<TYPE_DOUBLE>>) {
                        if (const_v == -std::numeric_limits<double>::infinity()) {
                            EXPECT_EQ(arg._fixed_values.size(), 1);
                            auto it = arg._fixed_values.begin();
                            EXPECT_TRUE(Compare::equal(*it, const_v));
                        } else {
                            EXPECT_FALSE(arg.is_fixed_value_range());
                            EXPECT_TRUE(arg.is_scope_value_range());
                            EXPECT_EQ(arg.get_range_low_op(), doris::FILTER_LARGER_OR_EQUAL);
                            EXPECT_EQ(arg.get_range_high_op(), doris::FILTER_LESS_OR_EQUAL);
                            EXPECT_TRUE(arg.is_low_value_minimum());
                            EXPECT_TRUE(Compare::equal(arg.get_range_max_value(), const_v));
                        }
                    } else {
                        FAIL() << "unexpected type";
                    }
                },
                output_range);
    }

    // test greater
    for (auto const_v : test_values) {
        // std::cout << "test greater const_v=" << const_v << std::endl;
        auto local_state = std::make_shared<MockScanLocalState>(state.get(), op.get());
        ColumnValueRange<TYPE_DOUBLE> range("mock", false, 0, 0);
        local_state->_slot_id_to_value_range[SlotId] = std::make_pair(&slot_desc, range);

        auto slot_ref = std::make_shared<MockSlotRef>(0, std::make_shared<DataTypeFloat64>());
        auto fn_eq = MockFnCall::create("gt");
        auto const_val = std::make_shared<MockLiteral>(
                ColumnHelper::create_column_with_name<DataTypeFloat64>({const_v}));

        fn_eq->add_child(slot_ref);
        fn_eq->add_child(const_val);
        fn_eq->_node_type = TExprNodeType::BINARY_PRED;
        slot_ref->_slot_id = SlotId;
        EXPECT_FALSE(fn_eq->is_constant());

        auto ctx = VExprContext::create_shared(fn_eq);
        ctx->_prepared = true;
        ctx->_opened = true;

        vectorized::VExprSPtr new_root;
        auto conjunct_expr_root = ctx;
        auto st = local_state->_normalize_predicate(conjunct_expr_root->root(),
                                                    conjunct_expr_root.get(), new_root);
        EXPECT_TRUE(st.ok());
        EXPECT_EQ(new_root, nullptr);

        EXPECT_TRUE(local_state->_slot_id_to_value_range.contains(SlotId));
        const auto& output_range = local_state->_slot_id_to_value_range[SlotId].second;
        /*
        _low_value = 90,
        _high_value = nan,
        _low_op = doris::FILTER_LARGER,
        _high_op = doris::FILTER_LESS_OR_EQUAL,
        */
        std::visit(
                [&](auto&& arg) {
                    using T = std::decay_t<decltype(arg)>;
                    if constexpr (std::is_same_v<T, ColumnValueRange<TYPE_DOUBLE>>) {
                        if (std::isnan(const_v)) {
                            EXPECT_TRUE(arg.is_empty_value_range());
                        } else {
                            EXPECT_FALSE(arg.is_fixed_value_range());
                            EXPECT_TRUE(arg.is_scope_value_range());
                            EXPECT_EQ(arg.get_range_low_op(), doris::FILTER_LARGER);
                            EXPECT_EQ(arg.get_range_high_op(), doris::FILTER_LESS_OR_EQUAL);
                            EXPECT_TRUE(arg.is_high_value_maximum());
                            EXPECT_TRUE(Compare::equal(arg.get_range_min_value(), const_v));
                        }
                    } else {
                        FAIL() << "unexpected type";
                    }
                },
                output_range);
    }
    // test greater or equal
    for (auto const_v : test_values) {
        // std::cout << "test greater or equal const_v=" << const_v << std::endl;
        auto local_state = std::make_shared<MockScanLocalState>(state.get(), op.get());
        ColumnValueRange<TYPE_DOUBLE> range("mock", false, 0, 0);
        local_state->_slot_id_to_value_range[SlotId] = std::make_pair(&slot_desc, range);

        auto slot_ref = std::make_shared<MockSlotRef>(0, std::make_shared<DataTypeFloat64>());
        auto fn_eq = MockFnCall::create("ge");
        auto const_val = std::make_shared<MockLiteral>(
                ColumnHelper::create_column_with_name<DataTypeFloat64>({const_v}));

        fn_eq->add_child(slot_ref);
        fn_eq->add_child(const_val);
        fn_eq->_node_type = TExprNodeType::BINARY_PRED;
        slot_ref->_slot_id = SlotId;
        EXPECT_FALSE(fn_eq->is_constant());

        auto ctx = VExprContext::create_shared(fn_eq);
        ctx->_prepared = true;
        ctx->_opened = true;

        vectorized::VExprSPtr new_root;
        auto conjunct_expr_root = ctx;
        auto st = local_state->_normalize_predicate(conjunct_expr_root->root(),
                                                    conjunct_expr_root.get(), new_root);
        EXPECT_TRUE(st.ok());
        EXPECT_EQ(new_root, nullptr);

        EXPECT_TRUE(local_state->_slot_id_to_value_range.contains(SlotId));
        const auto& output_range = local_state->_slot_id_to_value_range[SlotId].second;
        std::visit(
                [&](auto&& arg) {
                    using T = std::decay_t<decltype(arg)>;
                    if constexpr (std::is_same_v<T, ColumnValueRange<TYPE_DOUBLE>>) {
                        if (std::isnan(const_v)) {
                            EXPECT_EQ(arg._fixed_values.size(), 1);
                            auto it = arg._fixed_values.begin();
                            EXPECT_TRUE(Compare::equal(*it, const_v));
                        } else {
                            EXPECT_FALSE(arg.is_fixed_value_range());
                            EXPECT_TRUE(arg.is_scope_value_range());
                            EXPECT_EQ(arg.get_range_low_op(), doris::FILTER_LARGER_OR_EQUAL);
                            EXPECT_EQ(arg.get_range_high_op(), doris::FILTER_LESS_OR_EQUAL);
                            EXPECT_TRUE(arg.is_high_value_maximum());
                            EXPECT_TRUE(Compare::equal(arg.get_range_min_value(), const_v));
                        }
                    } else {
                        FAIL() << "unexpected type";
                    }
                },
                output_range);
    }
}
} // namespace doris::pipeline
