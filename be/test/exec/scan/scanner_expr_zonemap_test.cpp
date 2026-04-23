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

#include <memory>

#include "exec/pipeline/thrift_builder.h"
#include "exec/scan/scanner.h"
#include "exprs/vcompound_pred.h"
#include "exprs/vdirect_in_predicate.h"
#include "exprs/vruntimefilter_wrapper.h"
#include "runtime/runtime_profile.h"
#include "testutil/mock/mock_fn_call.h"
#include "testutil/mock/mock_literal_expr.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"

namespace doris {

namespace {

class TestScanner final : public Scanner {
public:
    TestScanner(RuntimeState* state, RuntimeProfile* profile) : Scanner(state, profile) {}

    using Scanner::_is_zone_map_eligible_expr;

    Status _get_block_impl(RuntimeState* state, Block* block, bool* eof) override {
        *eof = true;
        return Status::OK();
    }
};

VExprSPtr make_slot_ref(int slot_id = 0) {
    auto slot_ref = std::make_shared<MockSlotRef>(slot_id, std::make_shared<DataTypeInt32>());
    slot_ref->_slot_id = slot_id;
    slot_ref->_column_id = slot_id;
    slot_ref->set_expr_name("k0");
    return slot_ref;
}

std::shared_ptr<VectorizedFnCall> make_binary_pred(TExprOpcode::type opcode, int32_t literal) {
    auto fn = MockFnCall::create(opcode == TExprOpcode::LT ? "lt" : "gt");
    fn->add_child(make_slot_ref());
    fn->add_child(std::make_shared<MockLiteral>(
            ColumnHelper::create_column_with_name<DataTypeInt32>({literal})));
    fn->_node_type = TExprNodeType::BINARY_PRED;
    fn->_opcode = opcode;
    return fn;
}

std::shared_ptr<VectorizedFnCall> make_is_null_pred(const std::string& name) {
    auto fn = MockFnCall::create(name);
    fn->add_child(make_slot_ref());
    return fn;
}

std::shared_ptr<VRuntimeFilterWrapper> make_wrapper(const VExprSPtr& impl,
                                                    bool null_aware = false) {
    auto node = TExprNodeBuilder(TExprNodeType::SLOT_REF,
                                 TTypeDescBuilder()
                                         .set_types(TTypeNodeBuilder()
                                                            .set_type(TTypeNodeType::SCALAR)
                                                            .set_scalar_type(TPrimitiveType::INT)
                                                            .build())
                                         .build(),
                                 0)
                        .build();
    return VRuntimeFilterWrapper::create_shared(node, impl, 0.4, null_aware, 1);
}

} // namespace

TEST(ScannerExprZoneMapTest, accepts_supported_runtime_filter_shapes) {
    MockRuntimeState runtime_state;
    RuntimeProfile profile("scanner");
    TestScanner scanner(&runtime_state, &profile);

    auto binary_wrapper = make_wrapper(make_binary_pred(TExprOpcode::GT, 10));
    EXPECT_TRUE(scanner._is_zone_map_eligible_expr(binary_wrapper.get()));

    auto null_wrapper = make_wrapper(make_is_null_pred("is_null_pred"));
    EXPECT_TRUE(scanner._is_zone_map_eligible_expr(null_wrapper.get()));

    auto direct_in = std::make_shared<VDirectInPredicate>();
    direct_in->add_child(make_slot_ref());
    auto direct_in_wrapper = make_wrapper(direct_in);
    EXPECT_TRUE(scanner._is_zone_map_eligible_expr(direct_in_wrapper.get()));
}

TEST(ScannerExprZoneMapTest, rejects_null_aware_or_non_wrapper_filters) {
    MockRuntimeState runtime_state;
    RuntimeProfile profile("scanner");
    TestScanner scanner(&runtime_state, &profile);

    auto binary_wrapper = make_wrapper(make_binary_pred(TExprOpcode::GT, 10), true);
    EXPECT_FALSE(scanner._is_zone_map_eligible_expr(binary_wrapper.get()));

    auto non_wrapper = make_binary_pred(TExprOpcode::GT, 10);
    EXPECT_FALSE(scanner._is_zone_map_eligible_expr(non_wrapper.get()));
}

TEST(ScannerExprZoneMapTest, rejects_unsupported_runtime_filter_shapes) {
    MockRuntimeState runtime_state;
    RuntimeProfile profile("scanner");
    TestScanner scanner(&runtime_state, &profile);

    auto slot_vs_slot = MockFnCall::create("gt");
    slot_vs_slot->add_child(make_slot_ref(0));
    slot_vs_slot->add_child(make_slot_ref(1));
    slot_vs_slot->_node_type = TExprNodeType::BINARY_PRED;
    slot_vs_slot->_opcode = TExprOpcode::GT;
    auto slot_vs_slot_wrapper = make_wrapper(slot_vs_slot);
    EXPECT_FALSE(scanner._is_zone_map_eligible_expr(slot_vs_slot_wrapper.get()));

    auto compound = std::make_shared<VCompoundPred>();
    compound->_op = TExprOpcode::COMPOUND_NOT;
    compound->_opcode = TExprOpcode::COMPOUND_NOT;
    compound->_node_type = TExprNodeType::COMPOUND_PRED;
    compound->add_child(make_slot_ref());
    auto compound_wrapper = make_wrapper(compound);
    EXPECT_FALSE(scanner._is_zone_map_eligible_expr(compound_wrapper.get()));
}

} // namespace doris
