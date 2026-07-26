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
#include <random>
#include <vector>

#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"

namespace doris {

// A predicate whose per-row (bool, null) verdict is preset. Its execute_column_impl honors
// the selector exactly like real exprs (gathering down to the selected rows), so it drives
// the real execute_conjuncts_selective control flow. is_rf lets a test mark it a runtime-
// filter wrapper (forced onto the full-width path). execute_type returns nullptr so the
// execute_column wrapper skips its declared-vs-actual type check for this stub.
class FakeFilterExpr final : public VExpr {
public:
    FakeFilterExpr(std::vector<uint8_t> vals, std::vector<uint8_t> nulls, bool is_rf)
            : _vals(std::move(vals)), _nulls(std::move(nulls)), _is_rf(is_rf) {
        _node_type = TExprNodeType::BINARY_PRED;
        _data_type = std::make_shared<DataTypeUInt8>();
    }

    Status execute_column_impl(VExprContext*, const Block*, const Selector* selector, size_t count,
                               ColumnPtr& result_column) const override {
        auto nested = ColumnUInt8::create();
        auto null_map = ColumnUInt8::create();
        if (selector == nullptr) {
            for (size_t i = 0; i < count; ++i) {
                nested->insert_value(_vals[i]);
                null_map->insert_value(_nulls[i]);
            }
        } else {
            for (size_t j = 0; j < count; ++j) {
                const auto row = (*selector)[j];
                nested->insert_value(_vals[row]);
                null_map->insert_value(_nulls[row]);
            }
        }
        result_column = ColumnNullable::create(std::move(nested), std::move(null_map));
        return Status::OK();
    }

    DataTypePtr execute_type(const Block*) const override { return nullptr; }

    const std::string& expr_name() const override { return _name; }
    bool is_rf_wrapper() const override { return _is_rf; }
    // A real predicate over live columns is not constant; the default definition walks
    // children, and a childless fake would fold to constant and score cost 0 in
    // compute_conjunct_cost, which would defeat the adaptive-reorder cost gates below.
    bool is_constant() const override { return false; }

private:
    std::vector<uint8_t> _vals;
    std::vector<uint8_t> _nulls;
    bool _is_rf;
    std::string _name = "FakeFilterExpr";
};

class SelectiveConjunctsTest : public testing::Test {
protected:
    static VExprContextSPtr make_ctx(std::vector<uint8_t> vals, std::vector<uint8_t> nulls,
                                     bool is_rf = false) {
        return std::make_shared<VExprContext>(
                std::make_shared<FakeFilterExpr>(std::move(vals), std::move(nulls), is_rf));
    }

    // A block whose only job is to carry the row count; FakeFilterExpr ignores its columns.
    static Block make_block(size_t rows) {
        Block block;
        auto col = ColumnUInt8::create();
        for (size_t i = 0; i < rows; ++i) {
            col->insert_value(0);
        }
        block.insert({std::move(col), std::make_shared<DataTypeUInt8>(), "c0"});
        return block;
    }

    // Assert the selection-vector path and the full-width path agree on this input.
    static void expect_equivalent(size_t rows, const VExprContextSPtrs& ctxs,
                                  const std::vector<IColumn::Filter*>* filters) {
        Block block = make_block(rows);

        IColumn::Filter full(rows, 1);
        bool full_all = false;
        ASSERT_TRUE(VExprContext::execute_conjuncts(ctxs, filters, false, &block, &full, &full_all)
                            .ok());

        IColumn::Filter sel(rows, 1);
        bool sel_all = false;
        ASSERT_TRUE(VExprContext::execute_conjuncts_selective(ctxs, filters, &block, &sel, &sel_all)
                            .ok());

        EXPECT_EQ(full_all, sel_all);
        if (!full_all) {
            for (size_t i = 0; i < rows; ++i) {
                EXPECT_EQ(full[i], sel[i]) << "row " << i;
            }
        }
    }
};

TEST_F(SelectiveConjunctsTest, SingleConjunctMatchesFullWidth) {
    // vals: keep even rows. No nulls.
    std::vector<uint8_t> vals(100), nulls(100, 0);
    for (int i = 0; i < 100; ++i) {
        vals[i] = (i % 2 == 0);
    }
    expect_equivalent(100, {make_ctx(vals, nulls)}, nullptr);
}

TEST_F(SelectiveConjunctsTest, CheapThenExpensiveNarrows) {
    // First conjunct keeps 10% (drops below the 0.5 gather threshold), second keeps a subset;
    // the selective path must reach the same result as full-width.
    std::vector<uint8_t> v1(200), v2(200), n(200, 0);
    for (int i = 0; i < 200; ++i) {
        v1[i] = (i % 10 == 0); // keep 20 rows
        v2[i] = (i % 4 == 0);  // overlaps partially
    }
    expect_equivalent(200, {make_ctx(v1, n), make_ctx(v2, n)}, nullptr);
}

TEST_F(SelectiveConjunctsTest, NullRowsAreDropped) {
    // A row that is NULL for the predicate must be dropped (accept_null == false), the same
    // way full-width execute_filter drops it.
    std::vector<uint8_t> vals(150, 1), nulls(150, 0);
    for (int i = 0; i < 150; ++i) {
        vals[i] = (i % 3 != 0);  // structural selectivity
        nulls[i] = (i % 7 == 0); // some rows null
    }
    // Two conjuncts so the second runs selectively over survivors including the NULL logic.
    expect_equivalent(150, {make_ctx(vals, nulls), make_ctx(vals, nulls)}, nullptr);
}

TEST_F(SelectiveConjunctsTest, RuntimeFilterWrapperStaysFullWidth) {
    // An RF-wrapper conjunct must be handled full-width but still yield the same result.
    std::vector<uint8_t> v1(120), v2(120), n(120, 0);
    for (int i = 0; i < 120; ++i) {
        v1[i] = (i < 12); // cheap, keeps 10%
        v2[i] = (i % 2 == 0);
    }
    expect_equivalent(120, {make_ctx(v1, n, /*is_rf=*/true), make_ctx(v2, n)}, nullptr);
}

TEST_F(SelectiveConjunctsTest, CanFilterAllWhenEmpty) {
    // A conjunct that rejects everything -> can_filter_all on both paths.
    std::vector<uint8_t> vals(80, 0), nulls(80, 0);
    expect_equivalent(80, {make_ctx(vals, nulls)}, nullptr);
}

TEST_F(SelectiveConjunctsTest, PreexistingMaskFolded) {
    // A delete-style mask AND-ed in must be respected identically.
    std::vector<uint8_t> vals(100), nulls(100, 0);
    for (int i = 0; i < 100; ++i) {
        vals[i] = (i % 5 != 0);
    }
    IColumn::Filter mask(100, 1);
    for (int i = 0; i < 100; ++i) {
        mask[i] = (i < 30); // keep first 30
    }
    std::vector<IColumn::Filter*> filters = {&mask};
    expect_equivalent(100, {make_ctx(vals, nulls)}, &filters);
}

TEST_F(SelectiveConjunctsTest, RandomizedEquivalence) {
    // Fuzz: random rows, predicate count, selectivity, null rate, and RF flags. The selective
    // path must always agree with the full-width path (the core correctness contract).
    std::mt19937 rng(20260716);
    for (int iter = 0; iter < 500; ++iter) {
        const size_t rows = 1 + rng() % 400;
        const int npred = 1 + rng() % 5;
        VExprContextSPtrs ctxs;
        for (int p = 0; p < npred; ++p) {
            const double sel_rate = (rng() % 100) / 100.0;
            const double null_rate = (rng() % 25) / 100.0;
            std::vector<uint8_t> vals(rows), nulls(rows);
            for (size_t i = 0; i < rows; ++i) {
                vals[i] = ((rng() % 100) / 100.0) < sel_rate;
                nulls[i] = ((rng() % 100) / 100.0) < null_rate;
            }
            ctxs.push_back(make_ctx(std::move(vals), std::move(nulls), rng() % 4 == 0));
        }
        expect_equivalent(rows, ctxs, nullptr);
    }
}

class AdaptiveReorderTest : public testing::Test {
protected:
    // Build a conjunct with preset selectivity stats. `expensive` gives it a FUNCTION_CALL
    // child so compute_conjunct_cost scores it above the adaptive-reorder admission threshold
    // (a bare predicate scores ~2, below it); otherwise it stays a cheap comparison.
    static VExprContextSPtr with_stats(int64_t in, int64_t out, bool expensive,
                                       bool is_rf = false) {
        auto expr = std::make_shared<FakeFilterExpr>(std::vector<uint8_t> {},
                                                     std::vector<uint8_t> {}, is_rf);
        if (expensive) {
            auto fn = std::make_shared<FakeFilterExpr>(std::vector<uint8_t> {},
                                                       std::vector<uint8_t> {}, false);
            fn->set_node_type(TExprNodeType::FUNCTION_CALL);
            expr->add_child(fn);
        }
        auto ctx = std::make_shared<VExprContext>(expr);
        ctx->filter_runtime_stats().update(in, out);
        return ctx;
    }

    // Same, plus preset timing measurements. `elapsed_ns` accrues over `timed_rows`, which
    // must be >= kTimingMinRows for per_row_ns() to return the measurement (else the reorder
    // falls back on the static cost).
    static VExprContextSPtr with_timed_stats(int64_t in, int64_t out, bool expensive,
                                             int64_t elapsed_ns, int64_t timed_rows) {
        auto ctx = with_stats(in, out, expensive);
        auto& stats = ctx->filter_runtime_stats();
        stats.elapsed_ns = elapsed_ns;
        stats.input_rows_timed = timed_rows;
        return ctx;
    }
};

TEST_F(AdaptiveReorderTest, DroppedFractionFormula) {
    VExprContext::FilterRuntimeStats s;
    s.update(1000, 100); // dropped 900 of 1000
    EXPECT_DOUBLE_EQ(0.9, s.dropped_fraction());
    VExprContext::FilterRuntimeStats none;
    EXPECT_DOUBLE_EQ(0.0, none.dropped_fraction()); // no rows seen
    VExprContext::FilterRuntimeStats keep;
    keep.update(1000, 1000); // dropped nothing
    EXPECT_DOUBLE_EQ(0.0, keep.dropped_fraction());
}

TEST_F(AdaptiveReorderTest, NotReorderedWhenAllCheap) {
    // No conjunct is expensive by the static estimate -> the reorder is not worth its cost,
    // even though their selectivities differ.
    auto c1 = with_stats(1000, 900, /*expensive=*/false);
    auto c2 = with_stats(1000, 100, /*expensive=*/false);
    VExprContextSPtrs conjuncts = {c1, c2};
    EXPECT_FALSE(VExprContext::adaptive_reorder_conjuncts(conjuncts, 1000));
    EXPECT_EQ(c1.get(), conjuncts[0].get());
    EXPECT_EQ(c2.get(), conjuncts[1].get());
}

TEST_F(AdaptiveReorderTest, NotReorderedBelowMinRows) {
    // An expensive conjunct is present, but conjuncts have only seen 50 rows (< min_rows):
    // keep the static order until measurements are meaningful.
    auto c1 = with_stats(50, 45, /*expensive=*/true);
    auto c2 = with_stats(50, 5, /*expensive=*/false);
    VExprContextSPtrs conjuncts = {c1, c2};
    EXPECT_FALSE(VExprContext::adaptive_reorder_conjuncts(conjuncts, 100));
    EXPECT_EQ(c1.get(), conjuncts[0].get());
    EXPECT_EQ(c2.get(), conjuncts[1].get());
}

TEST_F(AdaptiveReorderTest, UnselectiveExpensiveSinksBelowCheap) {
    // An expensive predicate that turns out unselective (drops 10%) sorts after a cheap one
    // that drops half -- the whole point of the adaptive correction.
    auto expensive = with_stats(1000, 900, /*expensive=*/true); // cost~100, drop 0.1 -> ~1000
    auto cheap = with_stats(1000, 500, /*expensive=*/false);    // cost~2, drop 0.5 -> ~4
    VExprContextSPtrs conjuncts = {expensive, cheap};
    EXPECT_TRUE(VExprContext::adaptive_reorder_conjuncts(conjuncts, 1000));
    EXPECT_EQ(cheap.get(), conjuncts[0].get());
    EXPECT_EQ(expensive.get(), conjuncts[1].get());
}

TEST_F(AdaptiveReorderTest, SelectiveExpensiveBeatsUnselectiveExpensive) {
    // Two expensive predicates, same static cost; the one measured more selective runs first.
    auto sel = with_stats(1000, 50, /*expensive=*/true);    // drop 0.95
    auto unsel = with_stats(1000, 950, /*expensive=*/true); // drop 0.05
    VExprContextSPtrs conjuncts = {unsel, sel};
    EXPECT_TRUE(VExprContext::adaptive_reorder_conjuncts(conjuncts, 1000));
    EXPECT_EQ(sel.get(), conjuncts[0].get());
    EXPECT_EQ(unsel.get(), conjuncts[1].get());
}

TEST_F(AdaptiveReorderTest, DropNothingExpensiveSortsLast) {
    // A conjunct that drops nothing must not divide by zero; the eps floor sends it last.
    auto drops_nothing = with_stats(1000, 1000, /*expensive=*/true);
    auto drops_some = with_stats(1000, 100, /*expensive=*/true);
    VExprContextSPtrs conjuncts = {drops_nothing, drops_some};
    EXPECT_TRUE(VExprContext::adaptive_reorder_conjuncts(conjuncts, 1000));
    EXPECT_EQ(drops_some.get(), conjuncts[0].get());
    EXPECT_EQ(drops_nothing.get(), conjuncts[1].get());
}

TEST_F(AdaptiveReorderTest, RuntimeFilterWrapperExemptAndFirst) {
    // The RF wrapper is exempt from both gates and sorts first (key 0).
    auto cheap = with_stats(1000, 100, /*expensive=*/true);
    auto rf = with_stats(0, 0, /*expensive=*/false, /*is_rf=*/true);
    VExprContextSPtrs conjuncts = {cheap, rf};
    EXPECT_TRUE(VExprContext::adaptive_reorder_conjuncts(conjuncts, 1000));
    EXPECT_EQ(rf.get(), conjuncts[0].get());
    EXPECT_EQ(cheap.get(), conjuncts[1].get());
}

TEST_F(AdaptiveReorderTest, StatsAccumulateAcrossExecution) {
    // Running execute_conjuncts_selective must populate FilterRuntimeStats so a later
    // adaptive reorder has data. Use a low-selectivity first conjunct to trigger the
    // selective path on the second.
    std::vector<uint8_t> keep_few(200), n(200, 0);
    for (int i = 0; i < 200; ++i) {
        keep_few[i] = (i % 20 == 0); // keep 10
    }
    auto c1 = std::make_shared<VExprContext>(std::make_shared<FakeFilterExpr>(keep_few, n, false));
    std::vector<uint8_t> keep_half(200);
    for (int i = 0; i < 200; ++i) {
        keep_half[i] = (i % 2 == 0);
    }
    auto c2 = std::make_shared<VExprContext>(std::make_shared<FakeFilterExpr>(keep_half, n, false));
    VExprContextSPtrs conjuncts = {c1, c2};

    Block block;
    auto col = ColumnUInt8::create();
    for (int i = 0; i < 200; ++i) {
        col->insert_value(0);
    }
    block.insert({std::move(col), std::make_shared<DataTypeUInt8>(), "c0"});

    IColumn::Filter f(200, 1);
    bool can_all = false;
    ASSERT_TRUE(VExprContext::execute_conjuncts_selective(conjuncts, nullptr, &block, &f, &can_all)
                        .ok());
    // Both conjuncts saw rows; c1 over the full block, c2 over the survivors of c1.
    EXPECT_GT(c1->filter_runtime_stats().input_rows, 0);
    EXPECT_GT(c2->filter_runtime_stats().input_rows, 0);
    EXPECT_LT(c2->filter_runtime_stats().input_rows, c1->filter_runtime_stats().input_rows);
}

TEST_F(AdaptiveReorderTest, PerRowNsFormulaAndThreshold) {
    // Below kTimingMinRows the measurement is untrusted and per_row_ns returns 0.
    VExprContext::FilterRuntimeStats too_few;
    too_few.elapsed_ns = 100000;
    too_few.input_rows_timed = 100;
    EXPECT_DOUBLE_EQ(0.0, too_few.per_row_ns());

    // Above the threshold the mean nanoseconds per row is returned.
    VExprContext::FilterRuntimeStats enough;
    enough.elapsed_ns = 100000; // 100 us
    enough.input_rows_timed = 10000;
    EXPECT_DOUBLE_EQ(10.0, enough.per_row_ns()); // 100000 / 10000
}

TEST_F(AdaptiveReorderTest, MeasuredCostAdmitsWhenStaticUnderrates) {
    // A predicate that scores cheap statically (BINARY_PRED ~2) but is measured at 200ns/row
    // must still admit the reorder -- this is the whole point of adding measured cost. The
    // pair here is both statically cheap, so before this change adaptive_reorder_conjuncts
    // would refuse to reorder.
    auto slow = with_timed_stats(1000, 900, /*expensive=*/false,
                                 /*elapsed_ns=*/200 * 10000, /*timed_rows=*/10000);
    auto fast = with_timed_stats(1000, 100, /*expensive=*/false,
                                 /*elapsed_ns=*/2 * 10000, /*timed_rows=*/10000);
    VExprContextSPtrs conjuncts = {slow, fast};
    EXPECT_TRUE(VExprContext::adaptive_reorder_conjuncts(conjuncts, 1000));
    // Slow drops 10%, fast drops 90%. Keys: 200/0.1=2000 vs 2/0.9=2.2 -> fast first.
    EXPECT_EQ(fast.get(), conjuncts[0].get());
    EXPECT_EQ(slow.get(), conjuncts[1].get());
}

TEST_F(AdaptiveReorderTest, MeasuredCostSeparatesEqualStaticScores) {
    // Two FUNCTION_CALL predicates score the same statically (~102) but run 100x apart. The
    // measured per_row_ns lets the reorder distinguish them: whichever runs slower with worse
    // selectivity should sink.
    auto slow_call = with_timed_stats(1000, 500, /*expensive=*/true,
                                      /*elapsed_ns=*/500 * 10000, /*timed_rows=*/10000);
    auto fast_call = with_timed_stats(1000, 500, /*expensive=*/true,
                                      /*elapsed_ns=*/5 * 10000, /*timed_rows=*/10000);
    VExprContextSPtrs conjuncts = {slow_call, fast_call};
    EXPECT_TRUE(VExprContext::adaptive_reorder_conjuncts(conjuncts, 1000));
    // Same selectivity 0.5; key is per_row_ns/0.5 -> 1000 vs 10 -> fast first.
    EXPECT_EQ(fast_call.get(), conjuncts[0].get());
    EXPECT_EQ(slow_call.get(), conjuncts[1].get());
}

TEST_F(AdaptiveReorderTest, StaticCostFallbackWhenUntimed) {
    // When per_row_ns is 0 (samples below kTimingMinRows), sort_key falls back on the static
    // structural cost -- the pre-timing behavior, still driven by selectivity.
    auto slow_static = with_timed_stats(1000, 900, /*expensive=*/true,
                                        /*elapsed_ns=*/100000, /*timed_rows=*/100); // < 4096
    auto cheap_static = with_stats(1000, 500, /*expensive=*/false);
    VExprContextSPtrs conjuncts = {slow_static, cheap_static};
    EXPECT_TRUE(VExprContext::adaptive_reorder_conjuncts(conjuncts, 1000));
    // Static: expensive 102/0.1=1020, cheap 2/0.5=4 -> cheap first.
    EXPECT_EQ(cheap_static.get(), conjuncts[0].get());
    EXPECT_EQ(slow_static.get(), conjuncts[1].get());
}

TEST_F(AdaptiveReorderTest, SamplingCounterOnlyTimesEveryNthBatch) {
    // Sampling gate should_sample() must fire on batch 0 and every kTimingSampleEvery-th
    // batch after that, and stay quiet in between. Locks in the sample cadence so a naive
    // "always time" or "never time" regression is caught by the unit test.
    VExprContext::FilterRuntimeStats stats;
    std::vector<int64_t> sample_at;
    for (int64_t i = 0; i < 32; ++i) {
        if (stats.should_sample()) {
            sample_at.push_back(i);
        }
        stats.update(1, 1); // bumps sample_counter
    }
    const auto every = VExprContext::FilterRuntimeStats::kTimingSampleEvery;
    ASSERT_FALSE(sample_at.empty());
    EXPECT_EQ(0, sample_at.front());
    for (size_t i = 1; i < sample_at.size(); ++i) {
        EXPECT_EQ(every, sample_at[i] - sample_at[i - 1]);
    }
}

} // namespace doris
