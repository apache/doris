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

#include "storage/index/snii/query/internal/term_expansion.h"

#include <utility>
#include <vector>

#include "storage/index/snii/format/phrase_bigram.h"
#include "storage/index/snii/query/internal/docid_posting_reader.h"
#include "storage/index/snii/query/internal/docid_union.h"

namespace doris::snii::query::internal {

Status emit_expanded_docid_union(const reader::LogicalIndexReader& idx,
                                 std::string_view enum_prefix, const TermMatcher& matches,
                                 DocIdSink* const sink, int32_t max_expansions) {
    if (sink == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("term_expansion: null sink");
    }

    std::vector<ResolvedDocidPosting> postings;
    int32_t count = 0;
    RETURN_IF_ERROR(idx.visit_prefix_terms(
            enum_prefix, [&](reader::LogicalIndexReader::PrefixHit&& hit, bool* stop) {
                if (format::is_phrase_bigram_term(hit.term)) {
                    return Status::OK();
                }
                if (!matches(hit.term)) {
                    return Status::OK();
                }
                postings.push_back({std::move(hit.entry), hit.frq_base, hit.prx_base});
                ++count;
                *stop = max_expansions > 0 && count >= max_expansions;
                return Status::OK();
            }));
    return emit_docid_union(idx, postings, sink);
}

} // namespace doris::snii::query::internal
