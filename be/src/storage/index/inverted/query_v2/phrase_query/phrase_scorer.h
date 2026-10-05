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
#include "storage/index/inverted/query_v2/scorer.h"
#include "storage/index/inverted/query_v2/segment_postings.h"
#include "storage/index/query/phrase/phrase_verifier.h"
#include "storage/index/query/spi/scoring_context.h"

namespace doris::segment_v2::inverted_index::query_v2 {

template <typename TPostings>
class PhraseScorer;

template <typename TPostings>
using PhraseScorerPtr = std::shared_ptr<PhraseScorer<TPostings>>;

template <typename TPostings>
class PhraseScorer : public Scorer {
public:
    using IntersectionDocSetPtr = IntersectionPtr<TPostings, TPostings>;

    PhraseScorer(IntersectionDocSetPtr intersection_docset, std::vector<TPostings> terms,
                 size_t num_clauses, index_query::PhraseVerifier verifier,
                 index_query::ScoringContextPtr<float> similarity);
    ~PhraseScorer() override;

    // Clauses that share a postings object read its positions once per document.
    static ScorerPtr create(const std::vector<std::pair<size_t, TPostings>>& term_postings,
                            const index_query::ScoringContextPtr<float>& similarity,
                            const index_query::PhraseQueryOptions& options, uint32_t num_docs);

    uint32_t advance() override;
    uint32_t seek(uint32_t target) override;
    uint32_t doc() const override;
    uint32_t size_hint() const override;
    uint64_t cost() const override;
    uint32_t norm() const override;

    float score() override;

    bool phrase_match();

private:
    IntersectionDocSetPtr _intersection_docset;
    std::vector<TPostings> _terms;
    std::vector<std::vector<uint32_t>> _positions;
    index_query::PhraseVerifier _verifier;
    std::vector<index_query::PositionStream> _streams;
    size_t _num_clauses = 0;
    float _phrase_count = 0.0F;
    index_query::ScoringContextPtr<float> _similarity;
};

/// Instantiated once in phrase_scorer.cpp; suppresses per-TU implicit instantiation.
extern template class PhraseScorer<PostingsPtr>;
extern template class PhraseScorer<SegmentPostingsPtr>;

} // namespace doris::segment_v2::inverted_index::query_v2