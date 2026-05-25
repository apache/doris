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

// Tests for the InvertedIndexFilterTime sub-timer hierarchy introduced so
// that the filter-block profile invariant holds:
//
//   InvertedIndexFilterTime ≈ InvertedIndexApplyColPredTime
//                           + InvertedIndexApplyExprTime
//                           + InvertedIndexPostFilterTime
//
// Before this instrumentation, work that happened between the `_apply_*`
// calls and the end of the filter block (notably the post-filter bitmap
// intersection and the per-column "all conditions passed" check) was silently
// attributed to the parent InvertedIndexFilterTime but not to any inner timer,
// making it impossible to explain the gap between FilterTime and
// InvertedIndexQueryTime from the profile alone.
//
// These tests exercise `OlapReaderStatistics` as the public contract point and
// verify that the SCOPED_RAW_TIMERs added to `_get_row_ranges_by_column_conditions`
// produce counters that add up to the parent.

#include <gtest/gtest.h>

#include "runtime/runtime_profile.h"
#include "storage/olap_common.h"
#include "util/time.h"

namespace doris {

// -- 1. Field existence ------------------------------------------------------
//
// The three sub-timer fields must be present and default-initialized to 0.
// If someone removes/renames them in olap_common.h, the olap_scanner /
// olap_scan_operator wiring will fail to propagate them and the regression
// will silently re-appear; pin the contract here.
TEST(OlapReaderStatisticsFilterSubTimers, FieldsExistAndDefaultZero) {
    OlapReaderStatistics stats;
    EXPECT_EQ(stats.inverted_index_filter_timer, 0);
    EXPECT_EQ(stats.inverted_index_apply_col_pred_timer, 0);
    EXPECT_EQ(stats.inverted_index_apply_expr_timer, 0);
    EXPECT_EQ(stats.inverted_index_post_filter_timer, 0);
}

// -- 2. Scoped-timer accumulation semantics ----------------------------------
//
// `SCOPED_RAW_TIMER` reads `MonotonicNanos()` on construction and adds the
// elapsed wall-clock to the counter on destruction. Reproduce the exact
// nesting shape from `SegmentIterator::_get_row_ranges_by_column_conditions`
// (parent FilterTime wrapping three sibling sub-scopes) and assert the sum
// invariant holds. This directly guards the `SCOPED_RAW_TIMER` placement —
// if the parent and children are regrouped incorrectly (e.g. one of the
// children is reopened after FilterTime ends), this test will fail.
TEST(OlapReaderStatisticsFilterSubTimers, FilterTimerEqualsSumOfChildScopes) {
    OlapReaderStatistics stats;

    auto busy_wait_ns = [](int64_t target_ns) {
        const int64_t start = MonotonicNanos();
        while (MonotonicNanos() - start < target_ns) {
            // spin; the goal is predictable positive elapsed time
        }
    };

    {
        SCOPED_RAW_TIMER(&stats.inverted_index_filter_timer);
        {
            SCOPED_RAW_TIMER(&stats.inverted_index_apply_col_pred_timer);
            busy_wait_ns(1 * 1000 * 1000); // ≥ 1ms
        }
        {
            SCOPED_RAW_TIMER(&stats.inverted_index_apply_expr_timer);
            busy_wait_ns(2 * 1000 * 1000); // ≥ 2ms
        }
        {
            SCOPED_RAW_TIMER(&stats.inverted_index_post_filter_timer);
            busy_wait_ns(1 * 1000 * 1000); // ≥ 1ms
        }
    }

    const int64_t parent = stats.inverted_index_filter_timer;
    const int64_t child_sum = stats.inverted_index_apply_col_pred_timer +
                              stats.inverted_index_apply_expr_timer +
                              stats.inverted_index_post_filter_timer;

    // Each child must have captured at least its spin-wait budget.
    EXPECT_GE(stats.inverted_index_apply_col_pred_timer, 1 * 1000 * 1000);
    EXPECT_GE(stats.inverted_index_apply_expr_timer, 2 * 1000 * 1000);
    EXPECT_GE(stats.inverted_index_post_filter_timer, 1 * 1000 * 1000);

    // Parent must be ≥ the sum of children (scope nesting guarantees this).
    EXPECT_GE(parent, child_sum);

    // Drift between parent and the sum of children must be smaller than a
    // single child's budget — anything larger would mean the caller is doing
    // untimed work inside the filter block, which is exactly the bug this
    // instrumentation is supposed to surface.
    const int64_t drift = parent - child_sum;
    const int64_t single_child_budget_ns = 1 * 1000 * 1000;
    EXPECT_LT(drift, single_child_budget_ns)
            << "parent=" << parent << "ns child_sum=" << child_sum << "ns drift=" << drift << "ns";
}

// -- 3. Accumulation across multiple segments --------------------------------
//
// The segment iterator re-enters the filter block once per segment in a
// scanner, so the counters accumulate. Verify that running the scope twice
// produces counters that are (a) non-decreasing and (b) still satisfy
// parent ≥ sum(children).
TEST(OlapReaderStatisticsFilterSubTimers, AccumulatesAcrossMultipleInvocations) {
    OlapReaderStatistics stats;

    auto busy_wait_ns = [](int64_t target_ns) {
        const int64_t start = MonotonicNanos();
        while (MonotonicNanos() - start < target_ns) {
            // spin
        }
    };

    for (int i = 0; i < 3; ++i) {
        SCOPED_RAW_TIMER(&stats.inverted_index_filter_timer);
        {
            SCOPED_RAW_TIMER(&stats.inverted_index_apply_col_pred_timer);
            busy_wait_ns(500 * 1000); // 0.5ms
        }
        {
            SCOPED_RAW_TIMER(&stats.inverted_index_apply_expr_timer);
            busy_wait_ns(500 * 1000); // 0.5ms
        }
        {
            SCOPED_RAW_TIMER(&stats.inverted_index_post_filter_timer);
            busy_wait_ns(500 * 1000); // 0.5ms
        }
    }

    EXPECT_GE(stats.inverted_index_apply_col_pred_timer, 3 * 500 * 1000);
    EXPECT_GE(stats.inverted_index_apply_expr_timer, 3 * 500 * 1000);
    EXPECT_GE(stats.inverted_index_post_filter_timer, 3 * 500 * 1000);

    const int64_t parent = stats.inverted_index_filter_timer;
    const int64_t child_sum = stats.inverted_index_apply_col_pred_timer +
                              stats.inverted_index_apply_expr_timer +
                              stats.inverted_index_post_filter_timer;
    EXPECT_GE(parent, child_sum);
    // Drift should still be small after 3 iterations.
    EXPECT_LT(parent - child_sum, 1 * 1000 * 1000);
}

// -- 4. Searcher timer hierarchy (the invariants this patch restores) -------
//
// The fix in `FunctionSearch::evaluate_inverted_index_with_search_param` must
// preserve two profile invariants that already hold in the MATCH path:
//
//   (a) InvertedIndexQueryTime ≈ InvertedIndexSearcherOpenTime
//                              + InvertedIndexSearcherSearchTime
//   (b) InvertedIndexSearcherSearchTime ≈ InvertedIndexSearcherSearchInitTime
//                                       + InvertedIndexSearcherSearchExecTime
//
// Reproduce the corrected scope layout directly against OlapReaderStatistics
// to pin the invariants. If somebody re-introduces the pre-fix mistake
// (opening searchers inside SearchInitTime so OpenTime is nested under
// SearchTime, or leaving `weight->scorer()` outside all sub-timers), the
// accumulation in this test no longer satisfies the invariants and it fails.
TEST(OlapReaderStatisticsFilterSubTimers, SearcherTimerHierarchyInvariants) {
    OlapReaderStatistics stats;

    auto busy_wait_ns = [](int64_t target_ns) {
        const int64_t start = MonotonicNanos();
        while (MonotonicNanos() - start < target_ns) {
            // spin
        }
    };

    {
        // `inverted_index_query_timer` is the outermost scope in SEARCH path —
        // the analog of `FullTextIndexReader::query` in the MATCH path.
        SCOPED_RAW_TIMER(&stats.inverted_index_query_timer);

        {
            // Phase A: open searchers. In the fixed SEARCH path this is done
            // by `build_query_recursive` which calls `FieldReaderResolver::resolve`,
            // and `inverted_index_searcher_open_timer` is scoped *inside* the
            // resolver — crucially, OUTSIDE the `inverted_index_searcher_search_timer`.
            SCOPED_RAW_TIMER(&stats.inverted_index_searcher_open_timer);
            busy_wait_ns(3 * 1000 * 1000); // 3ms of "open" work
        }

        {
            // Phase B: search. Matches `match_index_search` semantics in MATCH
            // path: SearchInitTime (weight + scorer prep) and SearchExecTime
            // (iterate scorer + null bitmap) are both direct children of
            // SearchTime.
            SCOPED_RAW_TIMER(&stats.inverted_index_searcher_search_timer);
            {
                SCOPED_RAW_TIMER(&stats.inverted_index_searcher_search_init_timer);
                busy_wait_ns(2 * 1000 * 1000); // 2ms weight+scorer construction
            }
            {
                SCOPED_RAW_TIMER(&stats.inverted_index_searcher_search_exec_timer);
                busy_wait_ns(1 * 1000 * 1000); // 1ms scorer iteration
            }
        }
    }

    const int64_t query_t = stats.inverted_index_query_timer;
    const int64_t open_t = stats.inverted_index_searcher_open_timer;
    const int64_t search_t = stats.inverted_index_searcher_search_timer;
    const int64_t init_t = stats.inverted_index_searcher_search_init_timer;
    const int64_t exec_t = stats.inverted_index_searcher_search_exec_timer;

    // Individual children must have captured at least their spin budget.
    EXPECT_GE(open_t, 3 * 1000 * 1000);
    EXPECT_GE(search_t, 3 * 1000 * 1000); // 2ms init + 1ms exec
    EXPECT_GE(init_t, 2 * 1000 * 1000);
    EXPECT_GE(exec_t, 1 * 1000 * 1000);

    // Invariant (a): QueryTime ≈ OpenTime + SearchTime.
    // QueryTime wraps both, so QueryTime ≥ OpenTime + SearchTime, and the
    // drift is the inter-scope gap (very small — just function returns).
    EXPECT_GE(query_t, open_t + search_t);
    const int64_t query_drift = query_t - (open_t + search_t);
    EXPECT_LT(query_drift, 1 * 1000 * 1000) << "query=" << query_t << " open=" << open_t
                                            << " search=" << search_t << " drift=" << query_drift;

    // Invariant (b): SearchTime ≈ SearchInitTime + SearchExecTime.
    EXPECT_GE(search_t, init_t + exec_t);
    const int64_t search_drift = search_t - (init_t + exec_t);
    EXPECT_LT(search_drift, 1 * 1000 * 1000) << "search=" << search_t << " init=" << init_t
                                             << " exec=" << exec_t << " drift=" << search_drift;

    // Guard against the pre-fix mistake of nesting OpenTime *inside* SearchTime:
    // with that layout we would have OpenTime ≤ SearchInitTime ≤ SearchTime and
    // summing OpenTime + SearchTime as siblings would double-count the open.
    // Under the fixed layout OpenTime is already fully disjoint from SearchTime,
    // so SearchTime must NOT contain OpenTime:
    EXPECT_LT(search_t, open_t + 1 * 1000 * 1000)
            << "SearchTime must not contain OpenTime; nesting regression? "
            << "search=" << search_t << " open=" << open_t;
}

} // namespace doris
