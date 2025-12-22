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

#pragma once

#include "olap/rowset/segment_v2/inverted_index/query_v2/intersection.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/phrase_query/postings_with_offset.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/scorer.h"
#include "olap/rowset/segment_v2/inverted_index/similarity/similarity.h"

namespace doris::segment_v2::inverted_index::query_v2 {

template <typename TPostings>
class PhraseScorer;

template <typename TPostings>
using PhraseScorerPtr = std::shared_ptr<PhraseScorer<TPostings>>;

template <typename TPostings>
class PhraseScorer : public Scorer {
public:
    using IntersectionDocSetPtr =
            IntersectionPtr<PostingsWithOffsetPtr<TPostings>, PostingsWithOffsetPtr<TPostings>>;

    PhraseScorer(IntersectionDocSetPtr intersection_docset, size_t num_terms,
                 std::vector<uint32_t> left_positions, std::vector<uint32_t> right_positions,
                 uint32_t phrase_count, SimilarityPtr similarity, uint32_t slop)
            : _intersection_docset(std::move(intersection_docset)),
              _num_terms(num_terms),
              _left_positions(std::move(left_positions)),
              _right_positions(std::move(right_positions)),
              _phrase_count(phrase_count),
              _similarity(std::move(similarity)),
              _slop(slop) {}
    ~PhraseScorer() override = default;

    static ScorerPtr create(const std::vector<std::pair<size_t, TPostings>>& term_postings,
                            const SimilarityPtr& similarity, uint32_t slop, uint32_t num_docs) {
        return create_with_offset(term_postings, similarity, slop, 0, num_docs);
    }

    uint32_t advance() override;
    uint32_t seek(uint32_t target) override;
    uint32_t doc() const override;
    uint32_t size_hint() const override;
    uint64_t cost() const override;
    uint32_t norm() const override;

    float score() override;

    bool phrase_match();

private:
    static ScorerPtr create_with_offset(
            const std::vector<std::pair<size_t, TPostings>>& term_postings_with_offset,
            const SimilarityPtr& similarity, uint32_t slop, size_t offset, uint32_t num_docs);

    bool phrase_exists();
    uint32_t compute_phrase_count();
    void compute_phrase_match();
    size_t intersection_count(const std::vector<uint32_t>& left,
                              const std::vector<uint32_t>& right);
    bool intersection_exists(const std::vector<uint32_t>& left, const std::vector<uint32_t>& right);
    void intersection(std::vector<uint32_t>& left, const std::vector<uint32_t>& right);

    bool has_slop() const { return _slop > 0; }

    IntersectionDocSetPtr _intersection_docset;
    size_t _num_terms = 0;
    std::vector<uint32_t> _left_positions;
    std::vector<uint32_t> _right_positions;
    uint32_t _phrase_count = 0;
    SimilarityPtr _similarity;
    uint32_t _slop = 0;
};

template <typename TPostings>
inline void PhraseScorer<TPostings>::intersection(std::vector<uint32_t>& left,
                                                  const std::vector<uint32_t>& right) {
    size_t left_index = 0;
    size_t right_index = 0;
    size_t count = 0;
    const size_t left_len = left.size();
    const size_t right_len = right.size();
    while (left_index < left_len && right_index < right_len) {
        uint32_t left_val = left[left_index];
        uint32_t right_val = right[right_index];
        if (left_val < right_val) {
            ++left_index;
        } else if (left_val == right_val) {
            left[count] = left_val;
            ++count;
            ++left_index;
            ++right_index;
        } else {
            ++right_index;
        }
    }
    left.resize(count);
}

} // namespace doris::segment_v2::inverted_index::query_v2