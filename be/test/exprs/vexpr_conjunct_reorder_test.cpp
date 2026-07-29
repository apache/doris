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
#include <vector>

#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "exprs/vslot_ref.h"

namespace doris {

// Configurable expr node for testing conjunct cost/reorder: sets the node type and (for
// function nodes) the function name, and can mark itself constant so constant-subtree
// pruning in compute_conjunct_cost can be exercised. execute_column_impl is a stub -- the
// cost model only reads node_type/children, never evaluates.
class FakeCostExpr final : public VExpr {
public:
    FakeCostExpr(TExprNodeType::type node_type, std::string fn_name = "", bool is_const = false)
            : _is_const(is_const) {
        _node_type = node_type;
        _fn.name.function_name = std::move(fn_name);
        if (node_type == TExprNodeType::BINARY_PRED) {
            _opcode = TExprOpcode::EQ;
        }
    }

    Status execute_column_impl(VExprContext*, const Block*, const Selector*, size_t,
                               ColumnPtr&) const override {
        return Status::OK();
    }

    const std::string& expr_name() const override { return _name; }

    bool is_constant() const override { return _is_const || VExpr::is_constant(); }

private:
    bool _is_const;
    std::string _name = "FakeCostExpr";
};

static VExprSPtr make_slot(int slot_id) {
    auto slot = std::make_shared<VSlotRef>();
    slot->set_slot_id(slot_id);
    return slot;
}

static VExprSPtr make_literal() {
    return std::make_shared<FakeCostExpr>(TExprNodeType::STRING_LITERAL, "", true);
}

static VExprSPtr make_func(const std::string& fn_name, const std::vector<VExprSPtr>& children) {
    auto fn = std::make_shared<FakeCostExpr>(TExprNodeType::FUNCTION_CALL, fn_name);
    for (const auto& c : children) {
        fn->add_child(c);
    }
    return fn;
}

// col = 'x' : a bare-slot equality, the cheapest predicate shape.
static VExprSPtr make_bare_eq(int slot_id) {
    auto pred = std::make_shared<FakeCostExpr>(TExprNodeType::BINARY_PRED, "eq");
    pred->add_child(make_slot(slot_id));
    pred->add_child(make_literal());
    return pred;
}

// col IN (n literals) : still cheap (a hash lookup); the n constants must not inflate cost.
static VExprSPtr make_bare_in(int slot_id, int num_options) {
    auto pred = std::make_shared<FakeCostExpr>(TExprNodeType::IN_PRED, "in");
    pred->add_child(make_slot(slot_id));
    for (int i = 0; i < num_options; ++i) {
        pred->add_child(make_literal());
    }
    return pred;
}

// f(col) = 'x' : an expensive predicate whose value side is a function over a slot.
static VExprSPtr make_func_eq(const std::string& fn_name, int slot_id) {
    auto pred = std::make_shared<FakeCostExpr>(TExprNodeType::BINARY_PRED, "eq");
    pred->add_child(make_func(fn_name, {make_slot(slot_id), make_literal()}));
    pred->add_child(make_literal());
    return pred;
}

static VExprContextSPtr ctx_of(const VExprSPtr& root) {
    return std::make_shared<VExprContext>(root);
}

class ConjunctCostTest : public testing::Test {
protected:
    static double cost(const VExprSPtr& root) { return VExpr::compute_conjunct_cost(root); }
};

TEST_F(ConjunctCostTest, NullRootIsZero) {
    EXPECT_EQ(0.0, cost(nullptr));
}

TEST_F(ConjunctCostTest, BareEqCheaperThanFunctionPredicate) {
    // col = 'x' must be cheaper than split_by_string(col)[n] = 'x'.
    EXPECT_LT(cost(make_bare_eq(5)), cost(make_func_eq("split_by_string", 5)));
}

TEST_F(ConjunctCostTest, InSetSizeDoesNotInflateCost) {
    // col IN (3 consts) and col IN (500 consts) cost the same: the literals are constant
    // subtrees, folded once, not evaluated per row. Otherwise a big IN list would wrongly
    // sort after a cheap function.
    EXPECT_EQ(cost(make_bare_in(5, 3)), cost(make_bare_in(5, 500)));
}

TEST_F(ConjunctCostTest, InCheaperThanFunctionPredicate) {
    // Even a large IN list stays cheaper than a single function predicate.
    EXPECT_LT(cost(make_bare_in(5, 500)), cost(make_func_eq("substr", 5)));
}

TEST_F(ConjunctCostTest, NestedFunctionCostsMoreThanShallow) {
    // element_at(split_by_string(col), n) = 'x' does more per-row work than substr(col) = 'x'.
    auto shallow = make_func_eq("substr", 5);
    auto split = make_func("split_by_string", {make_slot(5), make_literal()});
    auto nested_pred = std::make_shared<FakeCostExpr>(TExprNodeType::BINARY_PRED, "eq");
    nested_pred->add_child(make_func("element_at", {split, make_literal()}));
    nested_pred->add_child(make_literal());
    EXPECT_GT(cost(nested_pred), cost(shallow));
}

class ConjunctReorderTest : public testing::Test {
protected:
    // Return the slot id referenced by the value side of predicate `i` after reordering,
    // for asserting order. Assumes each predicate's first child is the value side and its
    // leftmost leaf is a slot.
    static int leading_slot(const VExprContextSPtr& ctx) {
        const VExpr* node = ctx->root().get();
        while (!node->children().empty()) {
            node = node->children()[0].get();
        }
        return static_cast<const VSlotRef*>(node)->slot_id();
    }
};

TEST_F(ConjunctReorderTest, CheapPredicateMovesBeforeExpensive) {
    // Input order: [ expensive f(col1), cheap col2=const ]. After reorder the cheap one leads.
    VExprContextSPtrs conjuncts = {ctx_of(make_func_eq("split_by_string", 1)),
                                   ctx_of(make_bare_eq(2))};
    VExpr::reorder_conjuncts_by_cost(conjuncts);
    EXPECT_EQ(2, leading_slot(conjuncts[0]));
    EXPECT_EQ(1, leading_slot(conjuncts[1]));
}

TEST_F(ConjunctReorderTest, StableForEqualCost) {
    // Two equally cheap bare-eq predicates keep their original relative order (stable sort),
    // so reordering is deterministic and does not churn already-good plans.
    VExprContextSPtrs conjuncts = {ctx_of(make_bare_eq(7)), ctx_of(make_bare_eq(3)),
                                   ctx_of(make_bare_eq(9))};
    VExpr::reorder_conjuncts_by_cost(conjuncts);
    EXPECT_EQ(7, leading_slot(conjuncts[0]));
    EXPECT_EQ(3, leading_slot(conjuncts[1]));
    EXPECT_EQ(9, leading_slot(conjuncts[2]));
}

TEST_F(ConjunctReorderTest, MixedOrderingCheapestFirst) {
    // [ f(col1), col2 IN (...), g(col3), col4=const ] -> the two bare predicates lead,
    // in their original relative order, then the two function predicates.
    VExprContextSPtrs conjuncts = {ctx_of(make_func_eq("regexp_extract", 1)),
                                   ctx_of(make_bare_in(2, 50)), ctx_of(make_func_eq("substr", 3)),
                                   ctx_of(make_bare_eq(4))};
    VExpr::reorder_conjuncts_by_cost(conjuncts);
    // First two are the cheap bare predicates, col2 (IN) before col4 (=) by stable order.
    EXPECT_EQ(2, leading_slot(conjuncts[0]));
    EXPECT_EQ(4, leading_slot(conjuncts[1]));
    // Last two are the function predicates, col1 before col3 by stable order.
    EXPECT_EQ(1, leading_slot(conjuncts[2]));
    EXPECT_EQ(3, leading_slot(conjuncts[3]));
}

TEST_F(ConjunctReorderTest, EmptyAndSingleAreNoops) {
    VExprContextSPtrs empty;
    VExpr::reorder_conjuncts_by_cost(empty); // must not crash
    EXPECT_TRUE(empty.empty());

    VExprContextSPtrs single = {ctx_of(make_bare_eq(1))};
    VExpr::reorder_conjuncts_by_cost(single);
    ASSERT_EQ(1u, single.size());
    EXPECT_EQ(1, leading_slot(single[0]));
}

TEST_F(ConjunctReorderTest, NullContextDoesNotCrash) {
    // A null ctx (a skipped conjunct) is scored 0 and must not dereference.
    VExprContextSPtrs conjuncts = {ctx_of(make_func_eq("substr", 1)), nullptr,
                                   ctx_of(make_bare_eq(2))};
    VExpr::reorder_conjuncts_by_cost(conjuncts);
    // The null (cost 0) and the bare-eq (cheap) sort before the function predicate.
    EXPECT_EQ(3u, conjuncts.size());
    EXPECT_EQ(nullptr, conjuncts[0]);
}

} // namespace doris
