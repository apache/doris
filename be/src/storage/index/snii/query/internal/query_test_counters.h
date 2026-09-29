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

// Test-only operation counters for phrase-query and count-query paths. Reset them between tests and use them only from one test thread; release builds compile them out.
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
