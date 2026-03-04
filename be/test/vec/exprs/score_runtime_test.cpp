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

#include "vec/exprs/score_runtime.h"

#include <gtest/gtest.h>
#include <sys/resource.h>

#include <memory>

#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/Opcodes_types.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/virtual_slot_ref.h"

namespace doris::vectorized {

namespace {

class DummyExpr : public VExpr {
public:
    DummyExpr() { set_node_type(TExprNodeType::COMPOUND_PRED); }

    const std::string& expr_name() const override {
        static const std::string kName = "DummyExpr";
        return kName;
    }

    Status execute(VExprContext*, Block*, int*) const override { return Status::OK(); }
    Status execute_column(VExprContext* context, const Block* block, Selector* selector,
                          size_t count, ColumnPtr& result_column) const override {
        return Status::OK();
    }
};

VExprContextSPtr make_virtual_slot_context(int column_id) {
    TExprNode node;
    node.node_type = TExprNodeType::SLOT_REF;
    TTypeDesc type_desc;
    TTypeNode type_node;
    type_node.type = TTypeNodeType::SCALAR;
    TScalarType scalar_type;
    scalar_type.__set_type(TPrimitiveType::DOUBLE);
    type_node.__set_scalar_type(scalar_type);
    type_desc.types.push_back(type_node);
    node.__set_type(type_desc);
    node.num_children = 0;

    TSlotRef slot_ref;
    slot_ref.slot_id = -1;
    node.__set_slot_ref(slot_ref);
    node.__set_label("score");

    auto vir_slot = std::make_shared<VirtualSlotRef>(node);
    vir_slot->set_column_id(column_id);
    vir_slot->_virtual_column_expr = std::make_shared<DummyExpr>();
    return std::make_shared<VExprContext>(std::static_pointer_cast<VExpr>(vir_slot));
}

VExprContextSPtr make_dummy_context() {
    auto dummy = std::make_shared<DummyExpr>();
    return std::make_shared<VExprContext>(std::static_pointer_cast<VExpr>(dummy));
}

} // namespace

class ScoreRuntimeTest : public testing::Test {};

TEST_F(ScoreRuntimeTest, ConstructorSetsFields) {
    auto ctx = make_virtual_slot_context(3);
    auto runtime = ScoreRuntime::create_shared(ctx, true, 100);

    EXPECT_TRUE(runtime->is_asc());
    EXPECT_EQ(100, runtime->get_limit());
}

TEST_F(ScoreRuntimeTest, ConstructorDescOrder) {
    auto ctx = make_virtual_slot_context(0);
    auto runtime = ScoreRuntime::create_shared(ctx, false, 50);

    EXPECT_FALSE(runtime->is_asc());
    EXPECT_EQ(50, runtime->get_limit());
}

TEST_F(ScoreRuntimeTest, ConstructorZeroLimit) {
    auto ctx = make_virtual_slot_context(0);
    auto runtime = ScoreRuntime::create_shared(ctx, true, 0);

    EXPECT_EQ(0, runtime->get_limit());
}

TEST_F(ScoreRuntimeTest, NoScoreRangeByDefault) {
    auto ctx = make_virtual_slot_context(0);
    auto runtime = ScoreRuntime::create_shared(ctx, false, 10);

    EXPECT_FALSE(runtime->has_score_range_filter());
    EXPECT_FALSE(runtime->get_score_range_info().has_value());
}

TEST_F(ScoreRuntimeTest, SetScoreRangeGT) {
    auto ctx = make_virtual_slot_context(0);
    auto runtime = ScoreRuntime::create_shared(ctx, false, 10);

    runtime->set_score_range_info(TExprOpcode::GT, 0.5);

    EXPECT_TRUE(runtime->has_score_range_filter());
    ASSERT_TRUE(runtime->get_score_range_info().has_value());
    EXPECT_EQ(TExprOpcode::GT, runtime->get_score_range_info()->op);
    EXPECT_DOUBLE_EQ(0.5, runtime->get_score_range_info()->threshold);
}

TEST_F(ScoreRuntimeTest, SetScoreRangeGE) {
    auto ctx = make_virtual_slot_context(0);
    auto runtime = ScoreRuntime::create_shared(ctx, false, 10);

    runtime->set_score_range_info(TExprOpcode::GE, 1.0);

    ASSERT_TRUE(runtime->has_score_range_filter());
    EXPECT_EQ(TExprOpcode::GE, runtime->get_score_range_info()->op);
    EXPECT_DOUBLE_EQ(1.0, runtime->get_score_range_info()->threshold);
}

TEST_F(ScoreRuntimeTest, SetScoreRangeLT) {
    auto ctx = make_virtual_slot_context(0);
    auto runtime = ScoreRuntime::create_shared(ctx, false, 10);

    runtime->set_score_range_info(TExprOpcode::LT, 3.14);

    ASSERT_TRUE(runtime->has_score_range_filter());
    EXPECT_EQ(TExprOpcode::LT, runtime->get_score_range_info()->op);
    EXPECT_DOUBLE_EQ(3.14, runtime->get_score_range_info()->threshold);
}

TEST_F(ScoreRuntimeTest, SetScoreRangeLE) {
    auto ctx = make_virtual_slot_context(0);
    auto runtime = ScoreRuntime::create_shared(ctx, false, 10);

    runtime->set_score_range_info(TExprOpcode::LE, 99.9);

    ASSERT_TRUE(runtime->has_score_range_filter());
    EXPECT_EQ(TExprOpcode::LE, runtime->get_score_range_info()->op);
    EXPECT_DOUBLE_EQ(99.9, runtime->get_score_range_info()->threshold);
}

TEST_F(ScoreRuntimeTest, SetScoreRangeEQ) {
    auto ctx = make_virtual_slot_context(0);
    auto runtime = ScoreRuntime::create_shared(ctx, false, 10);

    runtime->set_score_range_info(TExprOpcode::EQ, 0.0);

    ASSERT_TRUE(runtime->has_score_range_filter());
    EXPECT_EQ(TExprOpcode::EQ, runtime->get_score_range_info()->op);
    EXPECT_DOUBLE_EQ(0.0, runtime->get_score_range_info()->threshold);
}

TEST_F(ScoreRuntimeTest, SetScoreRangeNegativeThreshold) {
    auto ctx = make_virtual_slot_context(0);
    auto runtime = ScoreRuntime::create_shared(ctx, false, 10);

    runtime->set_score_range_info(TExprOpcode::GT, -1.5);

    ASSERT_TRUE(runtime->has_score_range_filter());
    EXPECT_DOUBLE_EQ(-1.5, runtime->get_score_range_info()->threshold);
}

TEST_F(ScoreRuntimeTest, OverwriteScoreRangeInfo) {
    auto ctx = make_virtual_slot_context(0);
    auto runtime = ScoreRuntime::create_shared(ctx, false, 10);

    runtime->set_score_range_info(TExprOpcode::GT, 0.5);
    EXPECT_EQ(TExprOpcode::GT, runtime->get_score_range_info()->op);
    EXPECT_DOUBLE_EQ(0.5, runtime->get_score_range_info()->threshold);

    runtime->set_score_range_info(TExprOpcode::LE, 2.0);
    EXPECT_EQ(TExprOpcode::LE, runtime->get_score_range_info()->op);
    EXPECT_DOUBLE_EQ(2.0, runtime->get_score_range_info()->threshold);
}

TEST_F(ScoreRuntimeTest, PrepareSuccessWithVirtualSlotRef) {
    const int column_id = 5;
    auto ctx = make_virtual_slot_context(column_id);
    auto runtime = ScoreRuntime::create_shared(ctx, true, 10);

    RuntimeState state;
    RowDescriptor row_desc;
    Status st = runtime->prepare(&state, row_desc);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(column_id, runtime->get_dest_column_idx());
}

TEST_F(ScoreRuntimeTest, PrepareSuccessColumnIdZero) {
    auto ctx = make_virtual_slot_context(0);
    auto runtime = ScoreRuntime::create_shared(ctx, false, 20);

    RuntimeState state;
    RowDescriptor row_desc;
    Status st = runtime->prepare(&state, row_desc);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(0, runtime->get_dest_column_idx());
}

TEST_F(ScoreRuntimeTest, PrepareFailsWhenRootIsNotVirtualSlotRef) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    auto ctx = make_dummy_context();
    auto runtime = ScoreRuntime::create_shared(ctx, false, 10);

    RuntimeState state;
    RowDescriptor row_desc;
    ASSERT_DEATH(({
                     struct rlimit core_limit;
                     core_limit.rlim_cur = 0;
                     core_limit.rlim_max = 0;
                     setrlimit(RLIMIT_CORE, &core_limit);
                     auto st = runtime->prepare(&state, row_desc);
                 }),
                 "VirtualSlotRef");
}

TEST_F(ScoreRuntimeTest, CreateSharedReturnsValidPtr) {
    auto ctx = make_virtual_slot_context(0);
    auto runtime = ScoreRuntime::create_shared(ctx, true, 1);
    ASSERT_NE(nullptr, runtime);
}

TEST_F(ScoreRuntimeTest, FullWorkflow) {
    const int column_id = 7;
    auto ctx = make_virtual_slot_context(column_id);
    auto runtime = ScoreRuntime::create_shared(ctx, false, 42);

    EXPECT_FALSE(runtime->is_asc());
    EXPECT_EQ(42, runtime->get_limit());
    EXPECT_FALSE(runtime->has_score_range_filter());

    RuntimeState state;
    RowDescriptor row_desc;
    ASSERT_TRUE(runtime->prepare(&state, row_desc).ok());
    EXPECT_EQ(column_id, runtime->get_dest_column_idx());

    runtime->set_score_range_info(TExprOpcode::GT, 0.8);
    EXPECT_TRUE(runtime->has_score_range_filter());
    EXPECT_EQ(TExprOpcode::GT, runtime->get_score_range_info()->op);
    EXPECT_DOUBLE_EQ(0.8, runtime->get_score_range_info()->threshold);
}

} // namespace doris::vectorized
