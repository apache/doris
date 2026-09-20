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

#include "storage/index/inverted/query_v2/intersection.h"
#include "storage/index/inverted/query_v2/phrase_query/postings_with_offset.h"
#include "storage/index/inverted/query_v2/scorer.h"
#include "storage/index/query/spi/scoring_context.h"

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
                 index_query::ScoringContextPtr<float> similarity);
    ~PhraseScorer() override;

    // Sloppy queries must share a postings source for repeated terms.
    static ScorerPtr create(const std::vector<std::pair<size_t, TPostings>>& term_postings,
                            const index_query::ScoringContextPtr<float>& similarity, uint32_t slop,
                            uint32_t num_docs) {
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
            const index_query::ScoringContextPtr<float>& similarity, uint32_t slop, size_t offset,
            uint32_t num_docs);

    bool phrase_exists();
    uint32_t compute_phrase_count();
    template <bool CollectFrequency>
    void compute_phrase_match();

    IntersectionDocSetPtr _intersection_docset;
    size_t _num_terms = 0;
    size_t _first_clause_index = 0;
    std::vector<uint32_t> _left_positions;
    std::vector<uint32_t> _right_positions;
    float _phrase_count = 0.0F;
    bool _first_clause_is_last = false;
    index_query::ScoringContextPtr<float> _similarity;
    struct SloppyState;
    std::unique_ptr<SloppyState> _sloppy;
};

/// Instantiated once in phrase_scorer.cpp; suppresses per-TU implicit instantiation.
extern template class PhraseScorer<PostingsPtr>;
extern template class PhraseScorer<SegmentPostingsPtr>;

} // namespace doris::segment_v2::inverted_index::query_v2