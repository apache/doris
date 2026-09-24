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

#include "storage/index/snii/query/regexp_query.h"

#include <cstdint>
#include <string_view>
#include <vector>

#include "storage/index/snii/query/internal/term_expansion.h"

namespace doris::snii::query {

Status regexp_query(const reader::LogicalIndexReader& idx, std::string_view pattern,
                    std::vector<uint32_t>* const docids, int32_t max_expansions) {
    if (docids == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("regexp_query: null out");
    }
    docids->clear();
    index_query::VectorDocIdSink sink(*docids);
    return regexp_query(idx, pattern, &sink, max_expansions);
}

Status regexp_query(const reader::LogicalIndexReader& idx, std::string_view pattern,
                    std::vector<uint32_t>* const docids, QueryProfile* profile,
                    int32_t max_expansions) {
    QueryProfileScope profile_scope(idx.reader(), profile);
    return regexp_query(idx, pattern, docids, max_expansions);
}

Status regexp_query(const reader::LogicalIndexReader& idx, std::string_view pattern,
                    index_query::DocIdSink* const sink, int32_t max_expansions) {
    if (sink == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("regexp_query: null sink");
    }
    index_query::TermPattern term_pattern;
    RETURN_IF_ERROR(index_query::TermPattern::create(index_query::TermPatternKind::kRegexp, pattern,
                                                     &term_pattern));
    return internal::emit_expanded_docid_union(idx, term_pattern, sink, max_expansions);
}

} // namespace doris::snii::query
