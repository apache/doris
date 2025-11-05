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

#include "olap/rowset/segment_v2/index_query_context.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/bit_set_query/bit_set_scorer.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/const_score_query/const_score_scorer.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/null_bitmap_fetcher.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/phrase_query/phrase_scorer.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/scorer.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/weight.h"
#include "olap/rowset/segment_v2/inverted_index/util/string_helper.h"

namespace doris::segment_v2::inverted_index::query_v2 {

class PhraseWeight : public Weight {
public:
    PhraseWeight(IndexQueryContextPtr context, std::wstring field, std::vector<std::wstring> terms,
                 SimilarityPtr similarity, bool enable_scoring)
            : _context(std::move(context)),
              _field(std::move(field)),
              _terms(std::move(terms)),
              _similarity(std::move(similarity)),
              _enable_scoring(enable_scoring) {}
    ~PhraseWeight() override = default;

    ScorerPtr scorer(const QueryExecutionContext& ctx, const std::string& binding_key) override {
        auto phrase = phrase_scorer(ctx, binding_key);
        auto logical_field = logical_field_or_fallback(ctx, binding_key, _field);
        auto null_bitmap = FieldNullBitmapFetcher::fetch(ctx, logical_field);

        auto doc_bitset = std::make_shared<roaring::Roaring>();
        if (phrase) {
            uint32_t doc = phrase->doc();
            if (doc == TERMINATED) {
                doc = phrase->advance();
            }
            while (doc != TERMINATED) {
                doc_bitset->add(doc);
                doc = phrase->advance();
            }
        }

        auto bit_set =
                std::make_shared<BitSetScorer>(std::move(doc_bitset), std::move(null_bitmap));
        if (!phrase) {
            return bit_set;
        }
        // Wrap with const score for consistency with other non-scoring paths
        return std::make_shared<ConstScoreScorer<BitSetScorerPtr>>(std::move(bit_set));
    }

private:
    ScorerPtr phrase_scorer(const QueryExecutionContext& ctx, const std::string& binding_key) {
        auto reader = lookup_reader(_field, ctx, binding_key);
        if (!reader) {
            throw Exception(ErrorCode::NOT_FOUND, "Reader not found for field '{}'",
                            StringHelper::to_string(_field));
        }

        std::vector<std::pair<size_t, PositionPostings>> term_postings_list;
        for (size_t offset = 0; offset < _terms.size(); ++offset) {
            const auto& term = _terms[offset];
            auto t = make_term_ptr(_field.c_str(), term.c_str());
            auto iter = make_term_positions_ptr(reader.get(), t.get(), _enable_scoring,
                                                _context->io_ctx);
            if (iter) {
                auto segment_postings =
                        std::make_shared<SegmentPostings<TermPositionsPtr>>(std::move(iter));
                term_postings_list.emplace_back(offset, std::move(segment_postings));
            } else {
                return nullptr;
            }
        }
        return PhraseScorer<PositionPostings>::create(term_postings_list, _similarity, 0);
    }

    IndexQueryContextPtr _context;

    std::wstring _field;
    std::vector<std::wstring> _terms;
    SimilarityPtr _similarity;
    bool _enable_scoring = false;
};

} // namespace doris::segment_v2::inverted_index::query_v2
