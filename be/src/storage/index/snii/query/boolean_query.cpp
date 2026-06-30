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

#include "storage/index/snii/query/boolean_query.h"

#include <algorithm>
#include <string_view>
#include <utility>
#include <vector>

#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/query/docid_sink.h"
#include "storage/index/snii/query/internal/docid_conjunction.h"
#include "storage/index/snii/query/internal/docid_posting_reader.h"
#include "storage/index/snii/query/internal/docid_union.h"

namespace doris::snii::query {

namespace {

std::vector<std::string_view> unique_terms(const std::vector<std::string>& terms) {
    std::vector<std::string_view> out;
    out.reserve(terms.size());
    for (const std::string& term : terms) out.emplace_back(term);
    std::sort(out.begin(), out.end());
    out.erase(std::unique(out.begin(), out.end()), out.end());
    return out;
}

Status resolve_or_postings(const reader::LogicalIndexReader& idx,
                           const std::vector<std::string>& terms,
                           std::vector<internal::ResolvedDocidPosting>* postings) {
    postings->clear();
    for (std::string_view term : unique_terms(terms)) {
        bool found = false;
        format::DictEntry entry;
        uint64_t frq_base = 0;
        uint64_t prx_base = 0;
        RETURN_IF_ERROR(idx.lookup(term, &found, &entry, &frq_base, &prx_base));
        if (!found) continue;

        postings->push_back({std::move(entry), frq_base, prx_base});
    }
    return Status::OK();
}

} // namespace

Status boolean_or(const reader::LogicalIndexReader& idx, const std::vector<std::string>& terms,
                  std::vector<uint32_t>* docids) {
    if (docids == nullptr)
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("boolean_or: null out");
    docids->clear();
    if (terms.empty()) return Status::OK();

    std::vector<internal::ResolvedDocidPosting> postings;
    RETURN_IF_ERROR(resolve_or_postings(idx, terms, &postings));
    return internal::build_docid_union(idx, postings, docids);
}

Status boolean_or(const reader::LogicalIndexReader& idx, const std::vector<std::string>& terms,
                  std::vector<uint32_t>* docids, QueryProfile* profile) {
    QueryProfileScope profile_scope(idx.reader(), profile);
    return boolean_or(idx, terms, docids);
}

Status boolean_or(const reader::LogicalIndexReader& idx, const std::vector<std::string>& terms,
                  DocIdSink* sink) {
    if (sink == nullptr)
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("boolean_or: null sink");
    if (terms.empty()) return Status::OK();

    std::vector<internal::ResolvedDocidPosting> postings;
    RETURN_IF_ERROR(resolve_or_postings(idx, terms, &postings));
    return internal::emit_docid_union(idx, postings, sink);
}

Status boolean_and(const reader::LogicalIndexReader& idx, const std::vector<std::string>& terms,
                   std::vector<uint32_t>* docids) {
    if (docids == nullptr)
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("boolean_and: null out");
    docids->clear();
    if (terms.empty()) return Status::OK();

    io::BatchRangeFetcher round1(idx.reader());
    std::vector<internal::TermPlan> plans;
    bool all_present = false;
    RETURN_IF_ERROR(internal::plan_terms(idx, terms, &round1, &plans, &all_present,
                                         /*need_positions=*/false));
    if (!all_present) return Status::OK();
    if (round1.pending() > 0) RETURN_IF_ERROR(round1.fetch());
    RETURN_IF_ERROR(internal::open_preludes(round1, &plans,
                                            /*need_positions=*/false));
    return internal::build_docid_only_conjunction(idx, round1, plans, docids);
}

Status boolean_and(const reader::LogicalIndexReader& idx, const std::vector<std::string>& terms,
                   std::vector<uint32_t>* docids, QueryProfile* profile) {
    QueryProfileScope profile_scope(idx.reader(), profile);
    return boolean_and(idx, terms, docids);
}

} // namespace doris::snii::query
