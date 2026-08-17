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

#include "storage/segment/count_on_index_fastpath.h"

#include <gtest/gtest.h>

// Truth table for the G02 count-only fast-path caller guard. SegmentIterator
// fills CountOnIndexFastpathFacts from its state right before the index apply;
// this test pins that the ONLY admitted configuration is: COUNT_ON_INDEX agg +
// exactly one pushed-down MATCH expr + zero other filters + zero deletes +
// full row bitmap + zero row-id consumers + the no-read-data contract active.
// Each single deviation must veto the fast path (fall through to the
// row-accurate bitmap), because the fabricated [0, df) bitmap is only
// count-equivalent, never row-equivalent.
namespace doris::segment_v2 {

namespace {

// The one configuration that admits the fast path.
CountOnIndexFastpathFacts safe_facts() {
    CountOnIndexFastpathFacts f;
    f.is_count_on_index_agg = true;
    f.has_column_predicates = false;
    f.common_expr_count = 1;
    f.single_expr_is_match_pred = true;
    f.has_virtual_column_exprs = false;
    f.has_delete_predicates = false;
    f.segment_delete_bitmap_empty = true;
    f.has_col_id_predicates = false;
    f.has_topn_filters = false;
    f.has_external_row_ranges = false;
    f.row_bitmap_is_full = true;
    f.record_rowids = false;
    f.has_ann_topn = false;
    f.has_score_runtime = false;
    f.no_need_read_data_opt_enabled = true;
    f.keys_type_supported = true;
    return f;
}

} // namespace

TEST(CountOnIndexFastpath, AllGuardsSatisfiedAdmits) {
    EXPECT_TRUE(count_on_index_fastpath_safe(safe_facts()));
}

// Non-count context: no COUNT_ON_INDEX pushdown (the "context flag absent"
// case) -> the reader must take the normal bitmap path.
TEST(CountOnIndexFastpath, NotCountOnIndexVetoes) {
    auto f = safe_facts();
    f.is_count_on_index_agg = false;
    EXPECT_FALSE(count_on_index_fastpath_safe(f));
}

// Extra predicates beside the single MATCH veto: the count of one predicate
// is not the count of a conjunction.
TEST(CountOnIndexFastpath, ExtraPredicatesVeto) {
    {
        auto f = safe_facts();
        f.has_column_predicates = true;
        EXPECT_FALSE(count_on_index_fastpath_safe(f));
    }
    {
        auto f = safe_facts();
        f.common_expr_count = 2;
        EXPECT_FALSE(count_on_index_fastpath_safe(f));
    }
    {
        auto f = safe_facts();
        f.common_expr_count = 0;
        f.single_expr_is_match_pred = false;
        EXPECT_FALSE(count_on_index_fastpath_safe(f));
    }
    {
        // One expr, but a compound / non-MATCH root: leaf-level fabricated
        // bitmaps would be combined with sibling bitmaps.
        auto f = safe_facts();
        f.single_expr_is_match_pred = false;
        EXPECT_FALSE(count_on_index_fastpath_safe(f));
    }
    {
        auto f = safe_facts();
        f.has_col_id_predicates = true;
        EXPECT_FALSE(count_on_index_fastpath_safe(f));
    }
    {
        auto f = safe_facts();
        f.has_topn_filters = true;
        EXPECT_FALSE(count_on_index_fastpath_safe(f));
    }
    {
        auto f = safe_facts();
        f.has_virtual_column_exprs = true;
        EXPECT_FALSE(count_on_index_fastpath_safe(f));
    }
}

// Deletes veto: COUNT_ON_INDEX counts matching rows MINUS deleted rows, and a
// fabricated id range cannot participate in the delete-bitmap subtraction or
// in delete-predicate filtering (mirror of the V3 _lazy_init handling).
TEST(CountOnIndexFastpath, DeletesVeto) {
    {
        auto f = safe_facts();
        f.segment_delete_bitmap_empty = false;
        EXPECT_FALSE(count_on_index_fastpath_safe(f));
    }
    {
        auto f = safe_facts();
        f.has_delete_predicates = true;
        EXPECT_FALSE(count_on_index_fastpath_safe(f));
    }
}

// A pre-pruned or later-restricted row space vetoes: [0, df) only counts
// correctly against the full [0, num_rows) bitmap.
TEST(CountOnIndexFastpath, PrunedRowSpaceVetoes) {
    {
        auto f = safe_facts();
        f.row_bitmap_is_full = false; // condition cache / key ranges pruned
        EXPECT_FALSE(count_on_index_fastpath_safe(f));
    }
    {
        auto f = safe_facts();
        f.has_external_row_ranges = true;
        EXPECT_FALSE(count_on_index_fastpath_safe(f));
    }
}

// Consumers of REAL row ids veto.
TEST(CountOnIndexFastpath, RowIdConsumersVeto) {
    {
        auto f = safe_facts();
        f.record_rowids = true;
        EXPECT_FALSE(count_on_index_fastpath_safe(f));
    }
    {
        auto f = safe_facts();
        f.has_ann_topn = true;
        EXPECT_FALSE(count_on_index_fastpath_safe(f));
    }
    {
        auto f = safe_facts();
        f.has_score_runtime = true;
        EXPECT_FALSE(count_on_index_fastpath_safe(f));
    }
}

// The COUNT_ON_INDEX no-read-data contract must be active, otherwise column
// data would be materialized at fabricated row ids.
TEST(CountOnIndexFastpath, DataReadContractVetoes) {
    {
        auto f = safe_facts();
        f.no_need_read_data_opt_enabled = false;
        EXPECT_FALSE(count_on_index_fastpath_safe(f));
    }
    {
        auto f = safe_facts();
        f.keys_type_supported = false; // e.g. AGG keys / MOR without MOW
        EXPECT_FALSE(count_on_index_fastpath_safe(f));
    }
}

// --- G03 count-emission shortcut guard truth table --------------------------
// The shortcut replaces the per-rowid batch loop with a defaults-fill
// countdown; the ONLY admitted configuration is: reader answered from df +
// zero surviving evaluation stages + zero row-id/value consumers + pure
// countdown batch accounting + a schema-shaped block whose every column is
// defaults-fillable. Each single deviation must refuse (keep today's batch
// loop, which is always count-exact).

namespace {

CountEmitShortcutFacts safe_emit_facts() {
    CountEmitShortcutFacts f;
    f.count_fastpath_hit = true;
    f.needs_vec_eval = false;
    f.needs_short_eval = false;
    f.needs_expr_eval = false;
    f.has_remaining_col_predicates = false;
    f.has_remaining_common_exprs = false;
    f.has_delete_predicates = false;
    f.lazy_materialization_read = false;
    f.has_virtual_columns = false;
    f.record_rowids = false;
    f.has_read_limit = false;
    f.read_orderby_key_reverse = false;
    f.has_condition_cache_digest = false;
    f.block_shape_matches_schema = true;
    f.all_columns_emit_defaults = true;
    return f;
}

} // namespace

TEST(CountEmitShortcut, AllGuardsSatisfiedAdmits) {
    EXPECT_TRUE(count_emit_shortcut_safe(safe_emit_facts()));
}

// The reader must have ANSWERED via the fabricated count bitmap. A mere G02
// guard pass whose reader fell through to a row-accurate decode (multi-term,
// pruned bigram, CLucene index, query-cache hit) keeps today's emission.
TEST(CountEmitShortcut, NoReaderHitRefuses) {
    auto f = safe_emit_facts();
    f.count_fastpath_hit = false;
    EXPECT_FALSE(count_emit_shortcut_safe(f));
}

// Any surviving evaluation stage refuses: the shortcut emits unconditionally
// and cannot re-apply filters (e.g. an index-eval downgrade kept the expr).
TEST(CountEmitShortcut, SurvivingEvaluationRefuses) {
    {
        auto f = safe_emit_facts();
        f.needs_vec_eval = true;
        EXPECT_FALSE(count_emit_shortcut_safe(f));
    }
    {
        auto f = safe_emit_facts();
        f.needs_short_eval = true;
        EXPECT_FALSE(count_emit_shortcut_safe(f));
    }
    {
        auto f = safe_emit_facts();
        f.needs_expr_eval = true;
        EXPECT_FALSE(count_emit_shortcut_safe(f));
    }
    {
        auto f = safe_emit_facts();
        f.has_remaining_col_predicates = true;
        EXPECT_FALSE(count_emit_shortcut_safe(f));
    }
    {
        auto f = safe_emit_facts();
        f.has_remaining_common_exprs = true;
        EXPECT_FALSE(count_emit_shortcut_safe(f));
    }
    {
        auto f = safe_emit_facts();
        f.has_delete_predicates = true;
        EXPECT_FALSE(count_emit_shortcut_safe(f));
    }
    {
        auto f = safe_emit_facts();
        f.lazy_materialization_read = true;
        EXPECT_FALSE(count_emit_shortcut_safe(f));
    }
}

// Consumers of real row ids or per-row values refuse.
TEST(CountEmitShortcut, RowIdOrValueConsumersRefuse) {
    {
        auto f = safe_emit_facts();
        f.has_virtual_columns = true;
        EXPECT_FALSE(count_emit_shortcut_safe(f));
    }
    {
        auto f = safe_emit_facts();
        f.record_rowids = true;
        EXPECT_FALSE(count_emit_shortcut_safe(f));
    }
}

// Batch accounting must be a pure countdown: limits, reverse key-ordered
// reads, and condition-cache writes all keep today's loop.
TEST(CountEmitShortcut, NonCountdownBatchAccountingRefuses) {
    {
        auto f = safe_emit_facts();
        f.has_read_limit = true;
        EXPECT_FALSE(count_emit_shortcut_safe(f));
    }
    {
        auto f = safe_emit_facts();
        f.read_orderby_key_reverse = true;
        EXPECT_FALSE(count_emit_shortcut_safe(f));
    }
    {
        auto f = safe_emit_facts();
        f.has_condition_cache_digest = true;
        EXPECT_FALSE(count_emit_shortcut_safe(f));
    }
}

// The emitted block must be exactly the read schema and every column must be
// one today's path fills with defaults (no real read, no storage->schema
// cast, no version/lsn/tso rewrite).
TEST(CountEmitShortcut, BlockShapeOrColumnFillMismatchRefuses) {
    {
        auto f = safe_emit_facts();
        f.block_shape_matches_schema = false;
        EXPECT_FALSE(count_emit_shortcut_safe(f));
    }
    {
        auto f = safe_emit_facts();
        f.all_columns_emit_defaults = false;
        EXPECT_FALSE(count_emit_shortcut_safe(f));
    }
}

} // namespace doris::segment_v2
