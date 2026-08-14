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
#include <string>
#include <vector>

#include "exprs/vexpr.h"
#include "exprs/vslot_ref.h"

namespace doris {

// Configurable expr node for testing can_push_down_to_dict_filter: lets a test set the
// node type and (for function nodes) the function name, plus mark itself constant.
class FakeExpr final : public VExpr {
public:
    FakeExpr(TExprNodeType::type node_type, std::string fn_name = "", bool is_const = false)
            : _is_const(is_const) {
        _node_type = node_type;
        _fn.name.function_name = std::move(fn_name);
        // Predicate FakeExprs default to EQ so the common BINARY_PRED path passes the
        // opcode check in can_push_down_to_dict_filter; individual tests that want a
        // different opcode (e.g. LT/NE) can override via set_opcode.
        if (node_type == TExprNodeType::BINARY_PRED) {
            _opcode = TExprOpcode::EQ;
        } else if (node_type == TExprNodeType::IN_PRED) {
            _opcode = TExprOpcode::FILTER_IN;
        }
    }

    Status execute_column_impl(VExprContext*, const Block*, const Selector*, size_t,
                               ColumnPtr&) const override {
        return Status::OK();
    }

    const std::string& expr_name() const override { return _name; }

    bool is_constant() const override { return _is_const || VExpr::is_constant(); }

    // Stamp the planner verdict the FE would have sent, so tests can exercise the path where
    // BE trusts (or vetoes) the FE decision.
    void set_planner_verdict(bool verdict) { _can_dict_filter_from_planner = verdict; }

    void set_opcode(TExprOpcode::type opcode) { _opcode = opcode; }

private:
    bool _is_const;
    std::string _name = "FakeExpr";
};

// A VSlotRef whose node_type is SLOT_REF. The BE_TEST default VSlotRef ctor does not set
// node_type, so can_push_down_to_dict_filter (which relies on is_slot_ref()/node_type to
// find the referenced slot) would not recognize it; a real thrift-built VSlotRef always
// carries SLOT_REF, so setting it here matches production input.
class TestSlotRef final : public VSlotRef {
public:
    explicit TestSlotRef(int slot_id) {
        _node_type = TExprNodeType::SLOT_REF;
        set_slot_id(slot_id);
    }
};

static VExprSPtr make_slot(int slot_id) {
    auto slot = std::make_shared<TestSlotRef>(slot_id);
    return slot;
}

static VExprSPtr make_literal() {
    return std::make_shared<FakeExpr>(TExprNodeType::STRING_LITERAL, "", true);
}

// Build `pred_node(value_side, literal)`, e.g. eq(split_by_string(slot)[n], 'x').
static VExprSPtr make_pred(TExprNodeType::type pred_type, const VExprSPtr& value_side) {
    auto pred = std::make_shared<FakeExpr>(pred_type,
                                           pred_type == TExprNodeType::IN_PRED ? "in" : "eq");
    pred->add_child(value_side);
    pred->add_child(make_literal());
    return pred;
}

// Same as make_pred but stamps the FE planner verdict onto the predicate root, exercising the
// path where BE trusts the FE decision (and still applies its own defense-in-depth veto).
static VExprSPtr make_pred_with_verdict(TExprNodeType::type pred_type, const VExprSPtr& value_side,
                                        bool verdict) {
    auto pred = std::make_shared<FakeExpr>(pred_type,
                                           pred_type == TExprNodeType::IN_PRED ? "in" : "eq");
    pred->add_child(value_side);
    pred->add_child(make_literal());
    pred->set_planner_verdict(verdict);
    return pred;
}

static VExprSPtr make_func(const std::string& fn_name, const std::vector<VExprSPtr>& children) {
    auto fn = std::make_shared<FakeExpr>(TExprNodeType::FUNCTION_CALL, fn_name);
    for (const auto& c : children) {
        fn->add_child(c);
    }
    return fn;
}

class OrcDictPushDownTest : public testing::Test {
protected:
    static bool can(const VExprSPtr& root, int slot_id, bool allow_expr = true) {
        return VExpr::can_push_down_to_dict_filter(root, slot_id, allow_expr);
    }
};

TEST_F(OrcDictPushDownTest, BareSlotEq) {
    // col = 'x' : the original supported form, still accepted (even without a planner
    // verdict, so old-FE compatibility is preserved).
    EXPECT_TRUE(can(make_pred(TExprNodeType::BINARY_PRED, make_slot(5)), 5));
}

TEST_F(OrcDictPushDownTest, BareSlotIn) {
    EXPECT_TRUE(can(make_pred(TExprNodeType::IN_PRED, make_slot(5)), 5));
}

TEST_F(OrcDictPushDownTest, ExpressionWithoutVerdictRejected) {
    // An expression value side without a planner verdict is NOT dict-filterable: the
    // expression path is planner-authoritative (only the FE knows determinism, NULL
    // propagation, and UDF semantics), so BE never dict-filters an expression it
    // was not told about. Older FEs never stamped the flag; on new FE + new BE the
    // stamp is set on every candidate conjunct that FE deemed safe.
    auto split = make_func("split_by_string", {make_slot(5), make_literal()});
    auto element = make_func("element_at", {split, make_literal()});
    EXPECT_FALSE(can(make_pred(TExprNodeType::BINARY_PRED, element), 5));
    EXPECT_FALSE(can(make_pred(TExprNodeType::BINARY_PRED,
                               make_func("substr", {make_slot(3), make_literal()})),
                     3));
    EXPECT_FALSE(can(make_pred(TExprNodeType::BINARY_PRED,
                               make_func("regexp_extract", {make_slot(3), make_literal()})),
                     3));
}

TEST_F(OrcDictPushDownTest, WrongSlotRejected) {
    // Even with a planner verdict, the value side must derive from the target slot; a
    // predicate over col5 can't dict-filter against slot 8.
    auto split = make_func("split_by_string", {make_slot(5), make_literal()});
    EXPECT_FALSE(can(make_pred_with_verdict(TExprNodeType::BINARY_PRED, split, true), 8));
}

TEST_F(OrcDictPushDownTest, MultiSlotDerivedRejected) {
    // concat(col5, col8) = 'x' references two slots -> not single-slot-derived. Rejected
    // even when the FE stamped true (BE re-checks derives_from_single_slot).
    auto concat = make_func("concat", {make_slot(5), make_slot(8)});
    EXPECT_FALSE(can(make_pred_with_verdict(TExprNodeType::BINARY_PRED, concat, true), 5));
}

TEST_F(OrcDictPushDownTest, UnknownFunctionRejectedWithoutVerdict) {
    // Without a planner verdict, a UDF / unknown function is not dict-filterable: the
    // fallback only accepts bare column refs.
    auto expr = make_func("my_udf", {make_slot(5)});
    EXPECT_FALSE(can(make_pred(TExprNodeType::BINARY_PRED, expr), 5));
}

TEST_F(OrcDictPushDownTest, NullSensitiveRejectedWithoutVerdict) {
    // coalesce(col5, 'def') without a planner verdict: fallback rejects any expression.
    auto expr = make_func("coalesce", {make_slot(5), make_literal()});
    EXPECT_FALSE(can(make_pred(TExprNodeType::BINARY_PRED, expr), 5));
}

TEST_F(OrcDictPushDownTest, IsNullPredRejected) {
    // `col IS NULL` reaches here as IS_NULL_PRED, not a rewritable equality/IN.
    auto is_null = std::make_shared<FakeExpr>(TExprNodeType::IS_NULL_PRED, "is_null_pred");
    is_null->add_child(make_slot(5));
    EXPECT_FALSE(can(is_null, 5));
}

TEST_F(OrcDictPushDownTest, NullRoot) {
    EXPECT_FALSE(can(nullptr, 5));
}

TEST_F(OrcDictPushDownTest, NonEqBinaryOpcodeRejected) {
    // Only BINARY_PRED with opcode EQ is rewritten into a dict-code predicate; other
    // opcodes (LT/GT/NE/...) must be rejected structurally at the entry point so the
    // shape here matches what _rewrite_dict_conjuncts actually produces.
    auto lt_pred = std::make_shared<FakeExpr>(TExprNodeType::BINARY_PRED, "lt");
    lt_pred->set_opcode(TExprOpcode::LT);
    lt_pred->add_child(make_slot(5));
    lt_pred->add_child(make_literal());
    EXPECT_FALSE(can(lt_pred, 5));

    auto ne_pred = std::make_shared<FakeExpr>(TExprNodeType::BINARY_PRED, "ne");
    ne_pred->set_opcode(TExprOpcode::NE);
    ne_pred->add_child(make_slot(5));
    ne_pred->add_child(make_literal());
    EXPECT_FALSE(can(ne_pred, 5));
}

TEST_F(OrcDictPushDownTest, ExprDisabledFallsBackToBareSlot) {
    // With allow_expr=false (session variable off): a bare column ref still qualifies,
    // but a value-derived expression like split_by_string(col)[n] does not, even with a
    // planner verdict -- the session var is a hard kill switch for the expression path.
    EXPECT_TRUE(can(make_pred(TExprNodeType::BINARY_PRED, make_slot(5)), 5, false));
    auto split = make_func("split_by_string", {make_slot(5), make_literal()});
    auto element = make_func("element_at", {split, make_literal()});
    EXPECT_FALSE(can(make_pred(TExprNodeType::BINARY_PRED, element), 5, false));
    EXPECT_FALSE(can(make_pred_with_verdict(TExprNodeType::BINARY_PRED, element, true), 5, false));
}

TEST_F(OrcDictPushDownTest, PlannerVerdictTrustedForSafeExpr) {
    // FE stamped the conjunct as dict-filterable: BE trusts it (still confirming single-slot
    // derivation) even for functions it does not itself recognize.
    auto split = make_func("split_by_string", {make_slot(5), make_literal()});
    auto element = make_func("element_at", {split, make_literal()});
    EXPECT_TRUE(can(make_pred_with_verdict(TExprNodeType::BINARY_PRED, element, true), 5));
}

TEST_F(OrcDictPushDownTest, PlannerVerdictVetoedForNullBreakingFunc) {
    // Even if the FE (wrongly) stamped verdict=true, BE must veto a known NULL-breaking
    // function: coalesce(col, 'def') turns a NULL row into 'def', but the dictionary has no
    // NULL entry, so dict-filtering would drop NULL rows and return wrong results.
    auto expr = make_func("coalesce", {make_slot(5), make_literal()});
    EXPECT_FALSE(can(make_pred_with_verdict(TExprNodeType::BINARY_PRED, expr, true), 5));
}

TEST_F(OrcDictPushDownTest, PlannerVerdictVetoedForConcatWs) {
    // concat_ws skips NULL args (concat_ws(',', NULL) = ''), so it is not null-in-null-out
    // and must be vetoed even when the FE marked it dict-filterable.
    auto expr = make_func("concat_ws", {make_literal(), make_slot(5)});
    EXPECT_FALSE(can(make_pred_with_verdict(TExprNodeType::BINARY_PRED, expr, true), 5));
}

TEST_F(OrcDictPushDownTest, PlannerVerdictVetoedForNonDeterministicFunc) {
    // A non-deterministic function nested in the value side is vetoed regardless of the FE
    // verdict: dict evaluation caches one result per distinct value, which would be wrong.
    auto expr = make_func("concat", {make_slot(5), make_func("uuid", {})});
    EXPECT_FALSE(can(make_pred_with_verdict(TExprNodeType::BINARY_PRED, expr, true), 5));
}

TEST_F(OrcDictPushDownTest, PlannerVerdictFalseRejected) {
    // FE explicitly said no: reject even a structurally safe expression.
    auto expr = make_func("substr", {make_slot(5), make_literal()});
    EXPECT_FALSE(can(make_pred_with_verdict(TExprNodeType::BINARY_PRED, expr, false), 5));
}

TEST_F(OrcDictPushDownTest, PlannerVerdictWrongSlotRejected) {
    // FE verdict does not override the per-column requirement: a value side over slot 5 is
    // not dict-filterable against slot 8.
    auto split = make_func("split_by_string", {make_slot(5), make_literal()});
    EXPECT_FALSE(can(make_pred_with_verdict(TExprNodeType::BINARY_PRED, split, true), 8));
}

} // namespace doris
