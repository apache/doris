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

#include "snii/query/prefix_query.h"

#include <utility>
#include <vector>

#include "snii/format/phrase_bigram.h"
#include "snii/query/internal/term_expansion.h"

namespace snii::query {

using snii::reader::LogicalIndexReader;

doris::Status prefix_query(const LogicalIndexReader& idx, std::string_view prefix,
                           std::vector<uint32_t>* const docids, int32_t max_expansions) {
    if (docids == nullptr) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>(
                "prefix_query: null out");
    }
    docids->clear();
    VectorDocIdSink sink(*docids);
    return prefix_query(idx, prefix, &sink, max_expansions);
}

doris::Status prefix_query(const LogicalIndexReader& idx, std::string_view prefix,
                           std::vector<uint32_t>* const docids, QueryProfile* profile,
                           int32_t max_expansions) {
    QueryProfileScope profile_scope(idx.reader(), profile);
    return prefix_query(idx, prefix, docids, max_expansions);
}

doris::Status prefix_query(const LogicalIndexReader& idx, std::string_view prefix,
                           DocIdSink* const sink, int32_t max_expansions) {
    if (sink == nullptr) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>(
                "prefix_query: null sink");
    }

    return internal::emit_expanded_docid_union(
            idx, prefix,
            [](std::string_view term) { return !snii::format::is_phrase_bigram_term(term); }, sink,
            max_expansions);
}

} // namespace snii::query
