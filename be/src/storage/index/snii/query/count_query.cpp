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

#include "storage/index/snii/query/count_query.h"

#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/format/phrase_bigram.h"
#include "storage/index/snii/query/internal/query_test_counters.h"

namespace doris::snii::query {

using format::DictEntry;
using reader::LogicalIndexReader;

Status count_only_term_df(const LogicalIndexReader& idx, std::string_view term, uint64_t* count) {
    if (count == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("count_only_term_df: null out");
    }
    bool found = false;
    DictEntry entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    RETURN_IF_ERROR(idx.lookup(term, &found, &entry, &frq_base, &prx_base));
    *count = found ? entry.df : 0;
    SNII_QUERY_COUNT(count_fastpath_hits);
    return Status::OK();
}

Status count_only_two_term_phrase_bigram_df(const LogicalIndexReader& idx, const std::string& left,
                                            const std::string& right, bool* handled,
                                            uint64_t* count) {
    if (handled == nullptr || count == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "count_only_two_term_phrase_bigram_df: null out");
    }
    *handled = false;
    *count = 0;
    // The normal 2-term phrase path rejects positionless indexes with an error
    // BEFORE consulting bigrams; the count path must fall through so the same
    // error surfaces instead of a silently fabricated count.
    if (!idx.has_positions()) {
        return Status::OK();
    }
    if (!format::is_phrase_bigram_indexable_term(left) ||
        !format::is_phrase_bigram_indexable_term(right)) {
        return Status::OK();
    }

    bool found = false;
    DictEntry entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    RETURN_IF_ERROR(idx.lookup(format::make_phrase_bigram_term(left, right), &found, &entry,
                               &frq_base, &prx_base));
    if (!found) {
        // Miss is ambiguous on G01-pruned segments and merely "empty" on legacy
        // ones; both fall through so TryTwoTermPhraseBigram stays the single
        // owner of the miss semantics.
        return Status::OK();
    }
    // Docid membership IS the phrase answer for a materialized bigram (G01
    // part B stores them docs-only), so its df is the exact phrase doc count.
    *handled = true;
    *count = entry.df;
    SNII_QUERY_COUNT(count_fastpath_hits);
    return Status::OK();
}

} // namespace doris::snii::query
