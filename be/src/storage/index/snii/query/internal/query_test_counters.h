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

#include <cstdint>

// Deterministic op-count seam for the phrase-query hot path (T24/G01). The
// counters let the phrase UTs assert routing/complexity directly:
//   - expected_docids_build : how many times the multi-tail phrase-prefix branch
//                             materializes the expected-docid vector for a query.
//                             Hoisted out of the per-tail loop, so == 1 per query
//                             (was == tail_hits when rebuilt inside the loop).
//   - anchor_iterations     : total sparsest-anchor outer-enumeration size summed
//                             over candidate docs (== docs x min_span). Anchoring
//                             on the shortest per-doc span instead of the hardcoded
//                             phrase-position-0 span shrinks this when the leading
//                             exact term is high-frequency.
//   - monotonic_position_scans
//                           : number of non-anchor spans whose phrase-position
//                             lookup uses a forward-only scan instead of one
//                             binary search per anchor position.
//   - prefix_expected_doc_visits
//                           : total expected-doc iterations performed by the
//                             multi-tail phrase-prefix verification and result
//                             materialization path.
//   - count_fastpath_hits   : count-only (G02) answers produced from dict-entry
//                             df alone for a single term, with NO posting decode
//                             (count_query.cpp).
//   - resolved_term_entry_copies / moves
//                           : DictEntry ownership transfers performed by the two
//                             plan_resolved_terms overloads.
//   - resolved_term_payload_pointer_reuses
//                           : non-empty inline FRQ/PRX vectors whose data pointer
//                             survives an entry move into TermPlan.
//   - phrase_position_epoch_cache_hits / misses
//                           : same-document PhrasePositionLoader lookups served
//                             from the plan span cache vs loaded from its cursor.
//
// The seam is active only under SNII_QUERY_TEST_COUNTERS, which is auto-enabled by
// the library-wide BE_TEST define (be/CMakeLists.txt `if (MAKE_TEST)`) used to
// build doris_be_test. Because BE_TEST is applied to the whole BE tree, both the
// phrase_query.cpp increments AND the test translation unit that reads them observe
// the SAME process-wide singleton (the inline function below has one instance
// across every including TU). In a release build BE_TEST is undefined, the struct
// and singleton do not exist, and SNII_QUERY_COUNT/SNII_QUERY_ADD expand to
// ((void)0): zero overhead and NO global mutable state on the production query
// path.
//
// CONCURRENCY: the singleton is intentionally unsynchronized. It is a
// single-threaded, test-only seam -- one phrase query at a time -- and is never
// touched on the production path. Do NOT read or write it from concurrent tests.
// Reset it between test cases with `query_test_counters() = {}`.
#if defined(BE_TEST) && !defined(SNII_QUERY_TEST_COUNTERS)
#define SNII_QUERY_TEST_COUNTERS
#endif

#ifdef SNII_QUERY_TEST_COUNTERS

namespace doris::snii::query::internal {

struct QueryTestCounters {
    uint64_t expected_docids_build = 0;
    uint64_t anchor_iterations = 0;
    uint64_t monotonic_position_scans = 0;
    uint64_t prefix_expected_doc_visits = 0;
    uint64_t count_fastpath_hits = 0;
    uint64_t resolved_term_entry_copies = 0;
    uint64_t resolved_term_entry_moves = 0;
    uint64_t resolved_term_payload_pointer_reuses = 0;
    uint64_t phrase_position_epoch_cache_hits = 0;
    uint64_t phrase_position_epoch_cache_misses = 0;
};

// `inline` gives a single shared instance across all TUs that include this header
// (phrase_query.cpp and the test), so counter increments made in the library are
// visible to the test that reads them.
inline QueryTestCounters& query_test_counters() {
    static QueryTestCounters counters;
    return counters;
}

} // namespace doris::snii::query::internal

// NOLINTBEGIN(clang-diagnostic-unused-macros): expanded by phrase_query.cpp, not by this header's TU
#define SNII_QUERY_COUNT(field) (++::doris::snii::query::internal::query_test_counters().field)
#define SNII_QUERY_ADD(field, n) \
    (::doris::snii::query::internal::query_test_counters().field += (n))
// NOLINTEND(clang-diagnostic-unused-macros)

#else

#define SNII_QUERY_COUNT(field) ((void)0)
#define SNII_QUERY_ADD(field, n) ((void)0)

#endif // SNII_QUERY_TEST_COUNTERS
