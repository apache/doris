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

#include "storage/index/inverted/query_v2/phrase_query/phrase_scorer.h"

#include <algorithm>

namespace doris::segment_v2::inverted_index::query_v2 {

template <typename TPostings>
PhraseScorer<TPostings>::PhraseScorer(IntersectionDocSetPtr intersection_docset,
                                      std::vector<TermState> terms, size_t num_clauses,
                                      index_query::PhraseVerifier verifier,
                                      index_query::ScoringContextPtr<float> similarity,
                                      std::shared_ptr<const roaring::Roaring> candidates_owner)
        : _candidates_owner(std::move(candidates_owner)),
          _intersection_docset(std::move(intersection_docset)),
          _terms(std::move(terms)),
          _verifier(std::move(verifier)),
          _num_clauses(num_clauses),
          _similarity(std::move(similarity)) {
    if constexpr (std::is_same_v<TPostings, SegmentPostingsPtr>) {
        if (!_similarity && _verifier.can_stream()) {
            _streams.resize(_terms.size());
            _verifier.validate_stream(_streams.size());
        }
    }
}

template <typename TPostings>
PhraseScorer<TPostings>::~PhraseScorer() = default;

template <typename TPostings>
ScorerPtr PhraseScorer<TPostings>::create(
        const std::vector<std::pair<size_t, TPostings>>& term_postings,
        const index_query::ScoringContextPtr<float>& similarity,
        const index_query::PhraseQueryOptions& options, uint32_t num_docs) {
    const uint32_t slop = options.slop;
    std::vector<TPostings> clause_postings;
    std::vector<uint32_t> offsets;
    std::vector<uint64_t> costs;
    std::vector<TermState> terms;
    std::vector<size_t> clause_terms;
    for (const auto& [offset, postings] : term_postings) {
        clause_postings.push_back(postings);
        offsets.push_back(static_cast<uint32_t>(offset));
        costs.push_back(postings->cost());
        const auto term = std::ranges::find(terms, postings, &TermState::postings);
        clause_terms.push_back(static_cast<size_t>(term - terms.begin()));
        if (term == terms.end()) {
            terms.push_back({postings, {}});
        }
    }
    // A posting of no document, CLucene's answer for a term its dictionary lacks, empties the
    // phrase for the whole segment before it reaches the candidates.
    if (std::ranges::any_of(clause_postings,
                            [](const TPostings& postings) { return postings->size_hint() == 0; })) {
        return std::make_shared<EmptyScorer>();
    }
    index_query::PhraseVerifier verifier(std::move(clause_terms), offsets, costs, slop,
                                         options.ordered);
    if (options.candidate_rows_consumed != nullptr) {
        *options.candidate_rows_consumed = true;
    }
    auto scorer = std::make_shared<PhraseScorer<TPostings>>(
            make_intersection<TPostings>(clause_postings, num_docs, options.candidates),
            std::move(terms), clause_postings.size(), std::move(verifier), similarity,
            options.candidates_owner);
    if (scorer->doc() != TERMINATED && !scorer->phrase_match()) {
        scorer->advance();
    }
    return scorer;
}

template <typename TPostings>
uint32_t PhraseScorer<TPostings>::advance() {
    while (true) {
        uint32_t doc = _intersection_docset->advance();
        if (doc == TERMINATED || phrase_match()) {
            return doc;
        }
    }
}

template <typename TPostings>
uint32_t PhraseScorer<TPostings>::seek(uint32_t target) {
    assert(target >= doc());
    // Positions are forward-only, so seeking the current document must not read them again.
    if (target <= doc()) {
        return doc();
    }
    uint32_t doc = _intersection_docset->seek(target);
    if (doc == TERMINATED || phrase_match()) {
        return doc;
    }
    return advance();
}

template <typename TPostings>
uint32_t PhraseScorer<TPostings>::doc() const {
    return _intersection_docset->doc();
}

template <typename TPostings>
uint32_t PhraseScorer<TPostings>::size_hint() const {
    return _intersection_docset->size_hint();
}

template <typename TPostings>
uint64_t PhraseScorer<TPostings>::cost() const {
    return static_cast<uint64_t>(_intersection_docset->size_hint()) * 10 * _num_clauses;
}

template <typename TPostings>
uint32_t PhraseScorer<TPostings>::norm() const {
    return _intersection_docset->norm();
}

template <typename TPostings>
float PhraseScorer<TPostings>::score() {
    if (_similarity) {
        return _similarity->score(_phrase_count, norm());
    } else {
        return 1.0F;
    }
}

template <typename TPostings>
bool PhraseScorer<TPostings>::phrase_match() {
    if constexpr (std::is_same_v<TPostings, SegmentPostingsPtr>) {
        if (!_streams.empty()) {
            auto stream = _streams.begin();
            for (const TermState& term : _terms) {
                THROW_IF_ERROR(
                        stream->reset(term.postings->cursor(),
                                      static_cast<uint32_t>(term.postings->doc_set().ordinal())));
                ++stream;
            }
            bool matched = false;
            THROW_IF_ERROR(_verifier.verify_stream(std::span(_streams), &matched));
            _phrase_count = matched ? 1.0F : 0.0F;
            return matched;
        }
    }
    const auto load = [this](size_t term, index_query::PhrasePositionSpan* span) {
        auto& [postings, positions] = _terms[term];
        if constexpr (std::is_same_v<TPostings, SegmentPostingsPtr>) {
            positions.clear();
            RETURN_IF_ERROR(postings->cursor().append_positions(
                    static_cast<uint32_t>(postings->doc_set().ordinal()), 0, positions));
        } else {
            postings->positions_with_offset(0, positions);
        }
        *span = {positions.data(), positions.data() + positions.size()};
        return Status::OK();
    };
    THROW_IF_ERROR(_verifier.verify(load, _similarity != nullptr, &_phrase_count));
    return _phrase_count > 0.0F;
}

template class PhraseScorer<PostingsPtr>;
template class PhraseScorer<SegmentPostingsPtr>;

} // namespace doris::segment_v2::inverted_index::query_v2
