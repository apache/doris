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

#include "olap/rowset/segment_v2/inverted_index/query_v2/phrase_query/phrase_scorer.h"

namespace doris::segment_v2::inverted_index::query_v2 {

template <typename TPostings>
ScorerPtr PhraseScorer<TPostings>::create_with_offset(
        const std::vector<std::pair<size_t, TPostings>>& term_postings_with_offset,
        const SimilarityPtr& similarity, uint32_t slop, size_t offset) {
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

    using IntersectionType =
            Intersection<PostingsWithOffsetPtr<TPostings>, PostingsWithOffsetPtr<TPostings>>;
    auto intersection_docset = IntersectionType::create(postings_with_offsets);
    std::vector<uint32_t> left_positions(100);
    std::vector<uint32_t> right_positions(100);
    auto scorer = std::make_shared<PhraseScorer<TPostings>>(
            std::move(intersection_docset), num_docsets, std::move(left_positions),
            std::move(right_positions), 0, similarity, slop);
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
    assert(target > doc());
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
    compute_phrase_match();
    if (has_slop()) {
        // TODO: Implement sloppy phrase matching logic
        return 0;
    } else {
        return static_cast<uint32_t>(intersection_count(_left_positions, _right_positions));
    }
}

template <typename TPostings>
bool PhraseScorer<TPostings>::phrase_exists() {
    compute_phrase_match();
    if (has_slop()) {
        // TODO: Implement sloppy phrase matching logic
        return false;
    } else {
        return intersection_exists(_left_positions, _right_positions);
    }
}

template <typename TPostings>
void PhraseScorer<TPostings>::compute_phrase_match() {
    _intersection_docset->docset_mut_specialized(0)->postings(_left_positions);
    for (size_t i = 1; i < _num_terms - 1; ++i) {
        _intersection_docset->docset_mut_specialized(i)->postings(_right_positions);
        intersection(_left_positions, _right_positions);
        if (_left_positions.empty()) {
            return;
        }
    }
    _intersection_docset->docset_mut_specialized(_num_terms - 1)->postings(_right_positions);
}

template <typename TPostings>
size_t PhraseScorer<TPostings>::intersection_count(const std::vector<uint32_t>& left,
                                                   const std::vector<uint32_t>& right) {
    size_t left_index = 0;
    size_t right_index = 0;
    size_t count = 0;
    while (left_index < left.size() && right_index < right.size()) {
        uint32_t left_val = left[left_index];
        uint32_t right_val = right[right_index];
        if (left_val < right_val) {
            ++left_index;
        } else if (left_val == right_val) {
            ++count;
            ++left_index;
            ++right_index;
        } else {
            ++right_index;
        }
    }
    return count;
}

template <typename TPostings>
bool PhraseScorer<TPostings>::intersection_exists(const std::vector<uint32_t>& left,
                                                  const std::vector<uint32_t>& right) {
    size_t left_index = 0;
    size_t right_index = 0;
    while (left_index < left.size() && right_index < right.size()) {
        uint32_t left_val = left[left_index];
        uint32_t right_val = right[right_index];
        if (left_val < right_val) {
            ++left_index;
        } else if (left_val == right_val) {
            return true;
        } else {
            ++right_index;
        }
    }
    return false;
}

template class PhraseScorer<PositionPostings>;

} // namespace doris::segment_v2::inverted_index::query_v2