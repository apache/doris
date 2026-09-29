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

#include "storage/index/inverted/query_v2/boolean_query/listed_terms.h"

#include <algorithm>
#include <utility>

#include "common/exception.h"
#include "storage/index/query/exec/block_doc_set.h"
#include "storage/index/query/exec/collect_postings.h"
#include "storage/index/query/exec/cursor_chained_postings.h"
#include "storage/index/query/roaring_docid_sink.h"

namespace doris::segment_v2::inverted_index::query_v2 {

ListedTerms::ListedTerms(index_query::IndexSourcePtr source,
                         std::shared_ptr<roaring::Roaring> nulls)
        : _source(std::move(source)), _nulls(std::move(nulls)) {}

void ListedTerms::add(size_t clause, std::string term) {
    _clauses.push_back(clause);
    _terms.push_back(std::move(term));
}

bool ListedTerms::holds(size_t clause) const {
    return std::ranges::find(_clauses, clause) != _clauses.end();
}

void ListedTerms::open(bool conjunctive) {
    if (conjunctive) {
        for (const std::string& term : _terms) {
            bool held = false;
            THROW_IF_ERROR(_source->may_hold(term, &held));
            if (!held) {
                _cursors.resize(_terms.size());
                return;
            }
        }
    }
    THROW_IF_ERROR(_source->open_terms(_terms, /*positions=*/false, /*scoring=*/false, &_cursors));
}

bool ListedTerms::has_absent_term() const {
    return std::ranges::any_of(_cursors, [](const auto& cursor) { return cursor == nullptr; });
}

uint64_t ListedTerms::cheapest_doc_freq() const {
    uint64_t cheapest = UINT64_MAX;
    for (const auto& cursor : _cursors) {
        cheapest = std::min<uint64_t>(cheapest, cursor == nullptr ? 0 : cursor->doc_freq());
    }
    return cheapest;
}

index_query::TruthSet ListedTerms::conjunction(const std::vector<uint32_t>* candidates) {
    index_query::TruthSet result;
    if (has_absent_term()) {
        return result;
    }
    std::vector<index_query::CursorChainedPostings> terms;
    terms.reserve(_cursors.size());
    std::vector<index_query::ChainedPostings*> chain;
    for (const auto& cursor : _cursors) {
        terms.emplace_back(*cursor);
        chain.push_back(&terms.back());
    }
    std::vector<uint32_t> docs;
    THROW_IF_ERROR(index_query::chained_conjunction(chain, candidates, &docs));
    result.true_rows.addMany(docs.size(), docs.data());
    if (_nulls != nullptr) {
        result.null_rows = *_nulls;
    }
    return result;
}

index_query::TruthSet ListedTerms::disjunction() {
    index_query::TruthSet result;
    bool any_present = false;
    for (const auto& cursor : _cursors) {
        if (cursor != nullptr) {
            THROW_IF_ERROR(cursor->prefetch(nullptr, /*positions=*/false));
            any_present = true;
        }
    }
    if (!any_present) {
        return result;
    }
    THROW_IF_ERROR(_source->fetch_pending());
    index_query::RoaringDocIdSink sink(result.true_rows);
    for (const auto& cursor : _cursors) {
        if (cursor == nullptr) {
            continue;
        }
        index_query::BlockDocSet docs(*cursor);
        THROW_IF_ERROR(index_query::collect_postings<false>(docs, nullptr, sink,
                                                            [](uint32_t, uint32_t, uint32_t) {}));
    }
    if (_nulls != nullptr) {
        result.null_rows = *_nulls;
    }
    return result;
}

} // namespace doris::segment_v2::inverted_index::query_v2
