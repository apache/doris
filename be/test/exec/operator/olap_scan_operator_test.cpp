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

#include "exec/operator/olap_scan_operator.h"

#include <gtest/gtest.h>

#include <memory>

#include "common/object_pool.h"
#include "core/column/column_const.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "exprs/vexpr_context.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/QueryCache_types.h"
#include "testutil/column_helper.h"
#include "testutil/desc_tbl_builder.h"
#include "testutil/mock/mock_fn_call.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"

namespace doris {

class OlapScanConstantResultTest : public testing::Test {
protected:
    void SetUp() override {
        _state = std::make_shared<MockRuntimeState>();

        DescriptorTblBuilder desc_builder(&_pool);
        desc_builder.declare_tuple()
                << TupleDescBuilder::SlotType {std::make_shared<DataTypeInt32>(), "k1"}
                << TupleDescBuilder::SlotType {std::make_shared<DataTypeInt32>(), "v1"};
        _descs = desc_builder.build();
        ASSERT_NE(_descs, nullptr);

        auto* tuple_desc = _descs->get_tuple_descriptor(0);
        ASSERT_NE(tuple_desc, nullptr);
        ASSERT_EQ(tuple_desc->slots().size(), 2);
        _key_slot = tuple_desc->slots()[0];

        TOlapScanNode olap_scan_node;
        olap_scan_node.__set_tuple_id(0);
        olap_scan_node.__set_keyType(TKeysType::UNIQUE_KEYS);
        olap_scan_node.__set_key_column_name({"k1"});
        olap_scan_node.__set_key_column_type({TPrimitiveType::INT});

        TPlanNode plan_node;
        plan_node.__set_node_id(0);
        plan_node.__set_node_type(TPlanNodeType::OLAP_SCAN_NODE);
        plan_node.__set_num_children(0);
        plan_node.__set_row_tuples({0});
        plan_node.__set_olap_scan_node(olap_scan_node);

        _parent = std::make_shared<OlapScanOperatorX>(&_pool, plan_node, 0, *_descs, 1,
                                                      TQueryCacheParam {});
        _local_state = OlapScanLocalState::create_shared(_state.get(), _parent.get());
    }

    ObjectPool _pool;
    DescriptorTbl* _descs = nullptr;
    SlotDescriptor* _key_slot = nullptr;
    std::shared_ptr<MockRuntimeState> _state;
    std::shared_ptr<OlapScanOperatorX> _parent;
    std::shared_ptr<OlapScanLocalState> _local_state;
};

class OlapScanLikeConstantPatternTest : public OlapScanConstantResultTest {
protected:
    void check_pattern(bool wrap_const, uint8_t is_null, const std::string& value, bool opposite) {
        ColumnPtr column = ColumnHelper::create_nullable_column<DataTypeString>({value}, {is_null});
        if (wrap_const) {
            column = ColumnConst::create(column, 1);
        }
        auto pattern = MockFnCall::create("pattern");
        pattern->_mock_is_constant = true;
        pattern->_data_type = make_nullable(std::make_shared<DataTypeString>());
        pattern->set_const_expr_col(std::move(column));
        auto like = MockFnCall::create("like");
        like->add_child(std::make_shared<MockSlotRef>(0, std::make_shared<DataTypeString>()));
        like->add_child(pattern);
        VExprSPtr root = like;
        if (opposite) {
            auto not_expr = MockFnCall::create("not");
            not_expr->_node_type = TExprNodeType::COMPOUND_PRED;
            not_expr->add_child(like);
            root = not_expr;
        }
        auto ctx = VExprContext::create_shared(root);
        like->_fn_context_index = ctx->register_function_context(
                _state.get(), make_nullable(std::make_shared<DataTypeBool>()),
                {std::make_shared<DataTypeString>(), pattern->data_type()});
        auto pdt = PushDownType::ACCEPTABLE;
        StringRef constant_str;
        FunctionContext* fn_ctx = nullptr;
        auto st = _local_state->_should_push_down_function_filter(like.get(), ctx.get(),
                                                                  &constant_str, &fn_ctx, pdt);
        ASSERT_TRUE(st.ok()) << st.to_string();
        EXPECT_EQ(pdt, is_null ? PushDownType::UNACCEPTABLE : PushDownType::ACCEPTABLE);
        if (is_null) {
            check_expression_fallback(ctx.get());
        } else {
            EXPECT_EQ(constant_str.to_string(), value);
            EXPECT_EQ(fn_ctx, ctx->fn_context(like->_fn_context_index));
        }
    }

    void check_expression_fallback(VExprContext* ctx) {
        // Rejecting pushdown must leave both LIKE and NOT LIKE for expression evaluation.
        std::vector<std::shared_ptr<ColumnPredicate>> predicates;
        auto pdt = PushDownType::UNACCEPTABLE;
        auto st = _local_state->_normalize_function_filters(ctx, _key_slot, predicates, &pdt);
        ASSERT_TRUE(st.ok()) << st.to_string();
        EXPECT_TRUE(predicates.empty());
        EXPECT_EQ(pdt, PushDownType::UNACCEPTABLE);
    }
};

TEST_F(OlapScanLikeConstantPatternTest, NullPatternsRetainExpressionEvaluation) {
    for (bool wrap_const : {false, true}) {
        for (bool opposite : {false, true}) {
            check_pattern(wrap_const, 1, "", opposite);
            // A nonempty NULL payload must never be read as a LIKE pattern.
            check_pattern(wrap_const, 1, "ignored%", opposite);
        }
    }
}

TEST_F(OlapScanLikeConstantPatternTest, EmptyAndNonemptyPatternsRemainPushable) {
    for (bool wrap_const : {false, true}) {
        check_pattern(wrap_const, 0, "", false);
        check_pattern(wrap_const, 0, "prefix%", false);
    }
}

TEST_F(OlapScanLikeConstantPatternTest, BinaryPredicatePreservesNullConstant) {
    for (bool wrap_const : {false, true}) {
        ColumnPtr column = ColumnHelper::create_nullable_column<DataTypeInt32>({42}, {1});
        if (wrap_const) {
            column = ColumnConst::create(column, 1);
        }
        auto constant = MockFnCall::create("constant");
        constant->_mock_is_constant = true;
        constant->set_const_expr_col(std::move(column));
        auto eq = MockFnCall::create("eq");
        eq->add_child(std::make_shared<MockSlotRef>(0, std::make_shared<DataTypeInt32>()));
        eq->add_child(constant);
        auto ctx = VExprContext::create_shared(eq);
        Field value;
        EXPECT_EQ(_local_state->_should_push_down_binary_predicate(eq.get(), ctx.get(), value,
                                                                   {"eq"}),
                  PushDownType::ACCEPTABLE);
        EXPECT_TRUE(value.is_null());
    }
}

} // namespace doris
