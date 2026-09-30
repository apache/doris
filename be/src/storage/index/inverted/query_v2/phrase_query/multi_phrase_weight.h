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

#include "storage/index/inverted/query_v2/phrase_query/phrase_scorer.h"
#include "storage/index/inverted/query_v2/phrase_query/phrase_weight.h"
#include "storage/index/inverted/query_v2/postings/loaded_postings.h"
#include "storage/index/inverted/query_v2/segment_postings.h"
#include "storage/index/inverted/query_v2/union/simple_union.h"
#include "storage/index/inverted/query_v2/weight.h"

namespace doris::segment_v2::inverted_index::query_v2 {

constexpr uint32_t SPARSE_TERM_DOC_THRESHOLD = 100;

// A phrase whose clauses may hold several terms, any of which matches at the clause's position.
class MultiPhraseWeight final : public SlotPhraseWeight {
public:
    MultiPhraseWeight(std::wstring field, std::vector<TermInfo> term_infos,
                      index_query::PhraseQueryOptions options,
                      index_query::ScoringContextPtr<float> similarity, bool enable_scoring,
                      bool nullable)
            : SlotPhraseWeight(std::move(field), options, std::move(similarity), enable_scoring,
                               nullable),
              _term_infos(std::move(term_infos)) {}
    ~MultiPhraseWeight() override = default;

private:
    std::vector<PhraseSlot> _slots() const override {
        std::vector<PhraseSlot> slots;
        slots.reserve(_term_infos.size());
        for (const auto& term_info : _term_infos) {
            slots.push_back(
                    {.offset = static_cast<uint32_t>(term_info.position),
                     .terms = term_info.is_single_term()
                                      ? std::vector<std::string> {term_info.get_single_term()}
                                      : term_info.get_multi_terms()});
        }
        return slots;
    }

    ScorerPtr _streamed_scorer(index_query::IndexSource& source, uint32_t num_docs) override {
        std::vector<std::pair<size_t, PostingsPtr>> term_postings_list;
        for (const auto& term_info : _term_infos) {
            size_t offset = term_info.position;
            if (term_info.is_single_term()) {
                auto posting = open_postings(source, term_info.get_single_term(),
                                             /*positions=*/true, _enable_scoring, _similarity);
                if (posting) {
                    if (posting->size_hint() > SPARSE_TERM_DOC_THRESHOLD) {
                        auto loaded_posting = LoadedPostings::load(*posting);
                        term_postings_list.emplace_back(offset, std::move(loaded_posting));
                    } else {
                        term_postings_list.emplace_back(offset, std::move(posting));
                    }
                } else {
                    return std::make_shared<EmptyScorer>();
                }
            } else {
                const auto& terms = term_info.get_multi_terms();
                std::vector<PostingsPtr> postings;
                for (const auto& term : terms) {
                    auto posting = open_postings(source, term, /*positions=*/true, _enable_scoring,
                                                 _similarity);
                    if (posting) {
                        if (posting->size_hint() <= SPARSE_TERM_DOC_THRESHOLD) {
                            postings.push_back(LoadedPostings::load(*posting));
                        } else {
                            postings.push_back(posting);
                        }
                    }
                }
                if (postings.empty()) {
                    return std::make_shared<EmptyScorer>();
                }
                auto union_posting = SimpleUnion<PostingsPtr>::create(std::move(postings));
                term_postings_list.emplace_back(offset, std::move(union_posting));
            }
        }
        return PhraseScorer<PostingsPtr>::create(term_postings_list, _similarity, _options,
                                                 num_docs);
    }

    std::vector<TermInfo> _term_infos;
};

} // namespace doris::segment_v2::inverted_index::query_v2
