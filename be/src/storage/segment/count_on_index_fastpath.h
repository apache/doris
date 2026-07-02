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

#pragma once

#include <cstddef>
#include <cstdint>

// G02 count-only fast-path caller guard (functional core, unit-testable
// without a SegmentIterator).
//
// COUNT_ON_INDEX counts the rows of THIS segment that match the pushed-down
// predicates MINUS deleted rows: SegmentIterator seeds _row_bitmap with
// [0, num_rows), intersects the index result bitmap into it, then subtracts
// the MOW delete bitmap and applies delete predicates / row ranges, and the
// scan emits |_row_bitmap| default-valued rows that the agg counts. The SNII
// fast path replaces the index result with a FABRICATED [0, df) bitmap whose
// cardinality is exact but whose row ids are not real. That is only equal in
// observable behavior when, for this segment iterator:
//   1. only the COUNT matters (COUNT_ON_INDEX agg pushdown),
//   2. the single pushed-down MATCH predicate is the ONLY filter (nothing else
//      may intersect _row_bitmap before or after the index apply),
//   3. no rows are deleted (mirror of the V3 handling: _lazy_init subtracts
//      the per-segment delete bitmap AFTER the index apply, and delete
//      predicates filter later -- a fabricated id range cannot participate in
//      either subtraction),
//   4. nothing consumes REAL row ids (rowid recording, ANN topn, BM25
//      scoring, virtual columns), and
//   5. rows are emitted as defaults without reading column data at the
//      fabricated ids (the COUNT_ON_INDEX no-read-data contract must be
//      active: enable_no_need_read_data_opt + DUP_KEYS or MOW).
//
// SegmentIterator fills the facts from its state right before applying the
// index and only sets IndexQueryContext::count_on_index_fastpath when this
// predicate holds.
namespace doris::segment_v2 {

struct CountOnIndexFastpathFacts {
    // (1) count-only context.
    bool is_count_on_index_agg = false;
    // (2) single MATCH predicate, no other conjuncts.
    bool has_column_predicates = false;
    size_t common_expr_count = 0;
    bool single_expr_is_match_pred = false;
    bool has_virtual_column_exprs = false;
    // (3) deletes must be absent for this segment.
    bool has_delete_predicates = false;
    bool segment_delete_bitmap_empty = false;
    // (2 cont.) nothing else may prune the row space around the index apply.
    bool has_col_id_predicates = false;
    bool has_topn_filters = false;
    bool has_external_row_ranges = false;
    bool row_bitmap_is_full = false;
    // (4) consumers of real row ids.
    bool record_rowids = false;
    bool has_ann_topn = false;
    bool has_score_runtime = false;
    // (5) rows must be emitted as defaults (no data read at fabricated ids).
    bool no_need_read_data_opt_enabled = false;
    bool keys_type_supported = false;
};

inline bool count_on_index_fastpath_safe(const CountOnIndexFastpathFacts& f) {
    return f.is_count_on_index_agg && !f.has_column_predicates && f.common_expr_count == 1 &&
           f.single_expr_is_match_pred && !f.has_virtual_column_exprs && !f.has_delete_predicates &&
           f.segment_delete_bitmap_empty && !f.has_col_id_predicates && !f.has_topn_filters &&
           !f.has_external_row_ranges && f.row_bitmap_is_full && !f.record_rowids &&
           !f.has_ann_topn && !f.has_score_runtime && f.no_need_read_data_opt_enabled &&
           f.keys_type_supported;
}

// G03 count-emission shortcut guard (functional core, unit-testable without a
// SegmentIterator).
//
// After the G02 fast path answered the single MATCH predicate with a
// count-shaped bitmap, the only remaining work of the scan is to emit
// |_row_bitmap| default-valued rows batch by batch: the per-batch rowid
// iteration over the fabricated bitmap and the per-column no-read checks are
// pure overhead. The shortcut replaces them with a countdown that fills the
// block columns with defaults directly, in VStatisticsIterator-sized batches.
//
// That replacement is byte-for-byte equal to today's emission only when, at
// the end of _lazy_init:
//   1. the reader ACTUALLY answered from df (count_fastpath_hit) -- a mere
//      guard pass with a row-accurate decode keeps today's path untouched,
//   2. no evaluation stage survives (vec/short-circuit/expr eval, leftover
//      column predicates or common exprs, delete predicates, lazy
//      materialization),
//   3. nothing consumes real row ids or per-row values (virtual columns,
//      rowid recording),
//   4. batch accounting is a pure countdown (no read limit, no reverse
//      key-ordered read, no condition-cache writes), and
//   5. the block is exactly the read schema and EVERY column would take a
//      defaults fill in _read_columns_by_index (no real column read, no
//      storage->schema cast, no version/lsn/tso rewrite) -- checked
//      per-column by the iterator and summarized in one fact.
//
// Every fact is re-verified from live iterator state even though the G02
// facts guard already implies most of them: the shortcut independently
// refuses on any drift, falling through to today's emission (which is always
// count-exact).
struct CountEmitShortcutFacts {
    // (1) reader answered with a fabricated count bitmap.
    bool count_fastpath_hit = false;
    // (2) nothing evaluates or filters rows after the index apply.
    bool needs_vec_eval = false;
    bool needs_short_eval = false;
    bool needs_expr_eval = false;
    bool has_remaining_col_predicates = false;
    bool has_remaining_common_exprs = false;
    bool has_delete_predicates = false;
    bool lazy_materialization_read = false;
    // (3) consumers of real row ids / per-row values.
    bool has_virtual_columns = false;
    bool record_rowids = false;
    // (4) batch accounting must be a pure countdown.
    bool has_read_limit = false;
    bool read_orderby_key_reverse = false;
    bool has_condition_cache_digest = false;
    // (5) emitted block == read schema, all columns defaults-fillable.
    bool block_shape_matches_schema = false;
    bool all_columns_emit_defaults = false;
};

inline bool count_emit_shortcut_safe(const CountEmitShortcutFacts& f) {
    return f.count_fastpath_hit && !f.needs_vec_eval && !f.needs_short_eval && !f.needs_expr_eval &&
           !f.has_remaining_col_predicates && !f.has_remaining_common_exprs &&
           !f.has_delete_predicates && !f.lazy_materialization_read && !f.has_virtual_columns &&
           !f.record_rowids && !f.has_read_limit && !f.read_orderby_key_reverse &&
           !f.has_condition_cache_digest && f.block_shape_matches_schema &&
           f.all_columns_emit_defaults;
}

} // namespace doris::segment_v2

// Deterministic seam for the G03 count-emission shortcut, mirroring the SNII
// query seam (storage/index/snii/query/internal/query_test_counters.h):
//   - count_emit_shortcut_hits    : engage decisions that ADMITTED the
//                                   shortcut (incremented inside
//                                   _should_engage_count_emit_shortcut, which
//                                   _lazy_init calls once per iterator). Guard
//                                   fall-throughs leave it unchanged.
//   - count_emit_shortcut_batches : default-rows batches emitted by the
//                                   shortcut (== ceil(count / 65535) per
//                                   engaged iterator with a non-zero count).
//
// Active only under BE_TEST (library-wide define of doris_be_test); in a
// release build the macro expands to ((void)0): zero overhead, no global
// mutable state on the production path. The singleton is intentionally
// unsynchronized -- single-threaded test-only seam; reset between cases with
// `count_emit_test_counters() = {}`.
#if defined(BE_TEST) && !defined(SNII_COUNT_EMIT_TEST_COUNTERS)
#define SNII_COUNT_EMIT_TEST_COUNTERS
#endif

#ifdef SNII_COUNT_EMIT_TEST_COUNTERS

namespace doris::segment_v2::internal {

struct CountEmitTestCounters {
    uint64_t count_emit_shortcut_hits = 0;
    uint64_t count_emit_shortcut_batches = 0;
};

// `inline` gives a single shared instance across all TUs that include this
// header, so counter increments made in segment_iterator.cpp are visible to
// the test that reads them.
inline CountEmitTestCounters& count_emit_test_counters() {
    static CountEmitTestCounters counters;
    return counters;
}

} // namespace doris::segment_v2::internal

#define SNII_COUNT_EMIT_COUNT(field) \
    (++::doris::segment_v2::internal::count_emit_test_counters().field)

#else

#define SNII_COUNT_EMIT_COUNT(field) ((void)0)

#endif
