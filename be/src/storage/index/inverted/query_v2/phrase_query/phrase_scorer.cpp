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

#include <numeric>

#include "storage/index/query/phrase/exact_phrase_matcher.h"
#include "storage/index/query/phrase/sloppy_phrase_matcher.h"

namespace doris::segment_v2::inverted_index::query_v2 {

template <typename TPostings>
struct PhraseScorer<TPostings>::SloppyState {
    SloppyState(std::vector<TPostings> clause_postings, const std::vector<size_t>& identities,
                const std::vector<uint32_t>& offsets, uint32_t slop)
            : postings(std::move(clause_postings)),
              source_indices(identities),
              positions(postings.size()),
              spans(postings.size()),
              matcher(identities, offsets, slop, false) {}

    float match(bool collect_frequency) {
        for (size_t i = 0; i < postings.size(); ++i) {
            if (source_indices[i] != i) {
                spans[i] = spans[source_indices[i]];
                continue;
            }
            postings[i]->positions_with_offset(0, positions[i]);
            if (positions[i].empty()) {
                return 0.0F;
            }
            spans[i] = {positions[i].data(), positions[i].data() + positions[i].size()};
        }
        return matcher.match(spans, collect_frequency);
    }

    std::vector<TPostings> postings;
    std::vector<size_t> source_indices;
    std::vector<std::vector<uint32_t>> positions;
    std::vector<index_query::PhrasePositionSpan> spans;
    index_query::SloppyPhraseMatcher matcher;
};

template <typename TPostings>
PhraseScorer<TPostings>::PhraseScorer(IntersectionDocSetPtr intersection_docset, size_t num_terms,
                                      index_query::ScoringContextPtr<float> similarity)
        : _intersection_docset(std::move(intersection_docset)),
          _num_terms(num_terms),
          _first_clause_index(num_terms),
          _left_positions(100),
          _right_positions(100),
          _similarity(std::move(similarity)) {}

template <typename TPostings>
PhraseScorer<TPostings>::~PhraseScorer() = default;

template <typename TPostings>
ScorerPtr PhraseScorer<TPostings>::create_with_offset(
        const std::vector<std::pair<size_t, TPostings>>& term_postings_with_offset,
        const index_query::ScoringContextPtr<float>& similarity, uint32_t slop, size_t offset,
        uint32_t num_docs) {
    size_t max_offset = offset;
    for (const auto& [term_offset, _] : term_postings_with_offset) {
        max_offset = std::max(max_offset, term_offset + offset);
    }

    size_t num_docsets = term_postings_with_offset.size();
    std::vector<PostingsWithOffsetPtr<TPostings>> postings_with_offsets;
    postings_with_offsets.reserve(num_docsets);
    for (const auto& [term_offset, postings] : term_postings_with_offset) {
        auto adjusted_offset = static_cast<uint32_t>(max_offset - term_offset);
        auto postings_with_offset = std::make_shared<PostingsWithOffset<TPostings>>(
                std::move(postings), adjusted_offset);
        postings_with_offsets.emplace_back(std::move(postings_with_offset));
    }

    auto intersection_docset =
            make_intersection<PostingsWithOffsetPtr<TPostings>>(postings_with_offsets, num_docs);
    auto scorer = std::make_shared<PhraseScorer<TPostings>>(std::move(intersection_docset),
                                                            num_docsets, similarity);
    // Cost sorting must not change the original first clause's position multiplicity.
    if (slop == 0 && similarity) {
        for (size_t i = 0; i < num_docsets; ++i) {
            if (scorer->_intersection_docset->docset_mut_specialized(i) ==
                postings_with_offsets.front()) {
                scorer->_first_clause_index = i;
                scorer->_first_clause_is_last = i + 1 == num_docsets;
                break;
            }
        }
        DORIS_CHECK_LT(scorer->_first_clause_index, num_docsets);
    }
    if (slop > 0) {
        std::vector<TPostings> clause_postings;
        std::vector<uint32_t> offsets;
        std::vector<size_t> identities(num_docsets);
        std::iota(identities.begin(), identities.end(), 0);
        clause_postings.reserve(num_docsets);
        offsets.reserve(num_docsets);
        for (const auto& [term_offset, postings] : term_postings_with_offset) {
            clause_postings.push_back(postings);
            offsets.push_back(static_cast<uint32_t>(term_offset));
        }
        for (size_t i = 0; i < num_docsets; ++i) {
            for (size_t j = 0; j < i; ++j) {
                if (clause_postings[i] == clause_postings[j]) {
                    identities[i] = j;
                    break;
                }
            }
        }
        scorer->_sloppy = std::make_unique<SloppyState>(std::move(clause_postings), identities,
                                                        offsets, slop);
    }
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
    return static_cast<uint64_t>(_intersection_docset->size_hint()) * 10 * _num_terms;
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
    if (_sloppy) {
        _phrase_count = _sloppy->match(_similarity != nullptr);
        return _phrase_count > 0.0F;
    }
    if (_similarity) {
        uint32_t count = compute_phrase_count();
        _phrase_count = count;
        return count > 0;
    } else {
        return phrase_exists();
    }
}

template <typename TPostings>
uint32_t PhraseScorer<TPostings>::compute_phrase_count() {
    compute_phrase_match<true>();
    return index_query::count_two_term_phrase(
            {_left_positions.data(), _left_positions.data() + _left_positions.size()},
            {_right_positions.data(), _right_positions.data() + _right_positions.size()}, 0);
}

template <typename TPostings>
bool PhraseScorer<TPostings>::phrase_exists() {
    compute_phrase_match<false>();
    return index_query::contains_two_term_phrase(
            {_left_positions.data(), _left_positions.data() + _left_positions.size()},
            {_right_positions.data(), _right_positions.data() + _right_positions.size()}, 0);
}

template <typename TPostings>
template <bool CollectFrequency>
void PhraseScorer<TPostings>::compute_phrase_match() {
    _intersection_docset->docset_mut_specialized(0)->postings(_left_positions);
    for (size_t i = 1; i < _num_terms - 1; ++i) {
        _intersection_docset->docset_mut_specialized(i)->postings(_right_positions);
        bool preserve_first = false;
        if constexpr (CollectFrequency) {
            if (i == _first_clause_index) {
                _left_positions.swap(_right_positions);
            }
            preserve_first = i >= _first_clause_index;
        }
        const index_query::PhrasePositionSpan right {
                _right_positions.data(), _right_positions.data() + _right_positions.size()};
        if (preserve_first) {
            index_query::retain_exact_phrase_positions<true>(_left_positions, right);
        } else {
            index_query::retain_exact_phrase_positions<false>(_left_positions, right);
        }
        if (_left_positions.empty()) {
            return;
        }
    }
    _intersection_docset->docset_mut_specialized(_num_terms - 1)->postings(_right_positions);
    if constexpr (CollectFrequency) {
        if (_first_clause_is_last) {
            _left_positions.swap(_right_positions);
        }
    }
}

template class PhraseScorer<PostingsPtr>;
template class PhraseScorer<SegmentPostingsPtr>;

} // namespace doris::segment_v2::inverted_index::query_v2