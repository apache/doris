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

// Deterministic op-count seam for the SNII count-only path: count_fastpath_hits counts the
// count-only answers produced from a single term's dictionary df with no posting decode
// (count_query.cpp), so its tests assert the routing directly.
//
// The seam is active only under SNII_QUERY_TEST_COUNTERS, which the library-wide BE_TEST define
// (be/CMakeLists.txt `if (MAKE_TEST)`) of doris_be_test enables, so the library's increments and
// the test that reads them share the one process-wide singleton below. In a release build the
// struct and singleton do not exist and SNII_QUERY_COUNT expands to ((void)0).
//
// CONCURRENCY: the singleton is intentionally unsynchronized, a single-threaded test-only seam
// never touched on the production path. Reset it between test cases with
// `query_test_counters() = {}`.
#if defined(BE_TEST) && !defined(SNII_QUERY_TEST_COUNTERS)
#define SNII_QUERY_TEST_COUNTERS
#endif

#ifdef SNII_QUERY_TEST_COUNTERS

namespace doris::snii::query::internal {

struct QueryTestCounters {
    uint64_t count_fastpath_hits = 0;
};

// `inline` gives a single shared instance across every TU including this header.
inline QueryTestCounters& query_test_counters() {
    static QueryTestCounters counters;
    return counters;
}

} // namespace doris::snii::query::internal

// NOLINTBEGIN(clang-diagnostic-unused-macros): expanded by count_query.cpp, not by this header's TU
#define SNII_QUERY_COUNT(field) (++::doris::snii::query::internal::query_test_counters().field)
// NOLINTEND(clang-diagnostic-unused-macros)

#else

#define SNII_QUERY_COUNT(field) ((void)0)

#endif // SNII_QUERY_TEST_COUNTERS
