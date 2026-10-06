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

// Counts dictionary-only answers in BE tests; release builds omit the counters.
// The shared counters are unsynchronized, so use them on one thread and reset between tests.
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
