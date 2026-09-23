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
#include <cstdint>
#include <utility>
#include <vector>

#include "storage/index/query/docid_sink.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/query/internal/docid_conjunction.h"
#include "storage/index/snii/query/internal/docid_posting_reader.h"
#include "storage/index/snii/query/internal/docid_union.h"

namespace doris::snii::query {

namespace {

// A term the resident filter or the sampled term index rules out is dropped without a read. The
// other distinct terms resolve together, one read per wave of dictionary blocks.
Status resolve_or_postings(const reader::LogicalIndexReader& idx,
                           const std::vector<std::string>& terms,
                           std::vector<internal::ResolvedDocidPosting>* postings) {
    postings->clear();
    std::vector<std::string> distinct;
    for (const std::string& term : terms) {
        bool maybe_present = false;
        RETURN_IF_ERROR(idx.may_contain(term, &maybe_present));
        if (maybe_present) {
            distinct.push_back(term);
        }
    }
    if (distinct.empty()) {
        return Status::OK();
    }
    std::ranges::sort(distinct);
    distinct.erase(std::ranges::unique(distinct).begin(), distinct.end());
    std::vector<internal::ResolvedQueryTerm> resolved;
    std::vector<uint8_t> found;
    RETURN_IF_ERROR(internal::resolve_query_terms_batch(idx, distinct, &resolved, &found));
    for (size_t i = 0; i < distinct.size(); ++i) {
        if (found[i] != 0) {
            postings->push_back(
                    {std::move(resolved[i].entry), resolved[i].frq_base, resolved[i].prx_base});
        }
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
                  index_query::DocIdSink* sink) {
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
