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

#include "common/check.h"
#include "common/exception.h"
#include "storage/index/inverted/query_v2/postings/listed_walk.h"
#include "storage/index/inverted/query_v2/scored_rows_scorer.h"
#include "storage/index/query/exec/block_doc_set.h"
#include "storage/index/query/exec/collect_postings.h"
#include "storage/index/query/exec/cursor_chained_postings.h"
#include "storage/index/query/phrase/position_span.h"
#include "storage/index/query/roaring_docid_sink.h"

namespace doris::segment_v2::inverted_index::query_v2 {

ListedTerms::ListedTerms(index_query::IndexSourcePtr source,
                         std::shared_ptr<roaring::Roaring> nulls)
        : _source(std::move(source)), _nulls(std::move(nulls)) {}

void ListedTerms::add(size_t clause, std::string term,
                      index_query::ScoringContextPtr<float> similarity) {
    _clauses.push_back(clause);
    _terms.push_back(std::move(term));
    _similarities.push_back(std::move(similarity));
}

bool ListedTerms::holds(size_t clause) const {
    return std::ranges::find(_clauses, clause) != _clauses.end();
}

void ListedTerms::open(bool conjunctive, bool scoring) {
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
    // A scored conjunction reads the positions of the rows it lists; a scored disjunction reads
    // every term's frequencies and norms with its posting.
    THROW_IF_ERROR(_source->open_terms(_terms, /*positions=*/scoring && conjunctive,
                                       /*scoring=*/scoring && !conjunctive, &_cursors));
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

Status ListedTerms::_chain(const std::vector<uint32_t>* candidates, std::vector<uint32_t>* rows) {
    std::vector<index_query::CursorChainedPostings> terms;
    terms.reserve(_cursors.size());
    std::vector<index_query::ChainedPostings*> chain;
    for (const auto& cursor : _cursors) {
        terms.emplace_back(*cursor);
        chain.push_back(&terms.back());
    }
    return index_query::chained_conjunction(chain, candidates, rows);
}

index_query::TruthSet ListedTerms::conjunction(const std::vector<uint32_t>* candidates) {
    index_query::TruthSet result;
    if (has_absent_term()) {
        return result;
    }
    std::vector<uint32_t> docs;
    THROW_IF_ERROR(_chain(candidates, &docs));
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

namespace {

// Merges a term's postings into the rows scored so far, both ascending: a row both hold sums
// the scores, a row either holds alone keeps its own.
void merge_term(index_query::BlockDocSet& docs, index_query::ScoringContext<float>& similarity,
                std::vector<uint32_t>* rows, std::vector<float>* scores) {
    std::vector<uint32_t> merged_rows;
    std::vector<float> merged_scores;
    merged_rows.reserve(rows->size() + docs.size_hint());
    merged_scores.reserve(rows->size() + docs.size_hint());
    size_t i = 0;
    for (; !docs.exhausted(); docs.advance()) {
        const uint32_t doc = docs.doc();
        for (; i < rows->size() && (*rows)[i] < doc; ++i) {
            merged_rows.push_back((*rows)[i]);
            merged_scores.push_back((*scores)[i]);
        }
        float score = similarity.score(static_cast<float>(docs.freq()), docs.norm());
        if (i < rows->size() && (*rows)[i] == doc) {
            score += (*scores)[i];
            ++i;
        }
        merged_rows.push_back(doc);
        merged_scores.push_back(score);
    }
    merged_rows.insert(merged_rows.end(), rows->begin() + static_cast<std::ptrdiff_t>(i),
                       rows->end());
    merged_scores.insert(merged_scores.end(), scores->begin() + static_cast<std::ptrdiff_t>(i),
                         scores->end());
    rows->swap(merged_rows);
    scores->swap(merged_scores);
}

} // namespace

// The chain lists the rows, then every term reads their positions in one round and scores each
// row on the positions it holds there and the source's norm.
ScorerPtr ListedTerms::scored_conjunction() {
    if (has_absent_term()) {
        return std::make_shared<EmptyScorer>();
    }
    std::vector<uint32_t> rows;
    THROW_IF_ERROR(_chain(nullptr, &rows));
    if (rows.empty()) {
        return std::make_shared<EmptyScorer>();
    }
    for (const auto& cursor : _cursors) {
        THROW_IF_ERROR(cursor->prefetch(&rows, /*positions=*/true));
    }
    THROW_IF_ERROR(_source->fetch_pending());
    std::vector<uint32_t> norms;
    THROW_IF_ERROR(_source->encoded_norms(rows, &norms));
    std::vector<float> scores(rows.size(), 0.0F);
    for (size_t i = 0; i < _cursors.size(); ++i) {
        _similarities[i]->bind_norms(_source->norm_lengths());
        THROW_IF_ERROR(_cursors[i]->rewind());
        TermWalk walk(*_cursors[i], rows);
        for (size_t row = 0; row < rows.size(); ++row) {
            index_query::PhrasePositionSpan positions;
            THROW_IF_ERROR(walk.positions_of(row, rows[row], &positions));
            const auto frequency = static_cast<float>(positions.second - positions.first);
            scores[row] += _similarities[i]->score(frequency, norms[row]);
        }
    }
    return std::make_shared<ScoredRowsScorer>(std::move(rows), std::move(scores), _nulls);
}

// Every term reads its whole posting, with its frequencies and norms, in one round; a row's
// score sums the scores of the terms holding it, in clause order.
ScorerPtr ListedTerms::scored_disjunction() {
    bool any_present = false;
    for (const auto& cursor : _cursors) {
        if (cursor != nullptr) {
            THROW_IF_ERROR(cursor->prefetch(nullptr, /*positions=*/false));
            any_present = true;
        }
    }
    if (!any_present) {
        return std::make_shared<EmptyScorer>();
    }
    THROW_IF_ERROR(_source->fetch_pending());
    std::vector<uint32_t> rows;
    std::vector<float> scores;
    for (size_t i = 0; i < _cursors.size(); ++i) {
        if (_cursors[i] == nullptr) {
            continue;
        }
        _similarities[i]->bind_norms(_source->norm_lengths());
        index_query::BlockDocSet docs(*_cursors[i]);
        merge_term(docs, *_similarities[i], &rows, &scores);
    }
    return std::make_shared<ScoredRowsScorer>(std::move(rows), std::move(scores), _nulls);
}

} // namespace doris::segment_v2::inverted_index::query_v2
