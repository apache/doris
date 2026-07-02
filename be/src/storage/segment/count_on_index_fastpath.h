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

} // namespace doris::segment_v2
