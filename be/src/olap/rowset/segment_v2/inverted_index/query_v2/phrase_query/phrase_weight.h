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
#include "olap/rowset/segment_v2/inverted_index/query_v2/nullable_scorer.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/phrase_query/phrase_scorer.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/scorer.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/weight.h"
#include "olap/rowset/segment_v2/inverted_index/util/string_helper.h"

namespace doris::segment_v2::inverted_index::query_v2 {

constexpr uint32_t LOADED_POSTINGS_DOC_FREQ_THRESHOLD = 100;

class PhraseWeight : public Weight {
public:
    PhraseWeight(IndexQueryContextPtr context, std::wstring field, std::vector<TermInfo> term_infos,
                 SimilarityPtr similarity, bool enable_scoring, bool nullable)
            : _context(std::move(context)),
              _field(std::move(field)),
              _term_infos(std::move(term_infos)),
              _similarity(std::move(similarity)),
              _enable_scoring(enable_scoring),
              _nullable(nullable) {}
    ~PhraseWeight() override = default;

    ScorerPtr scorer(const QueryExecutionContext& ctx, const std::string& binding_key) override {
        auto scorer = phrase_scorer(ctx, binding_key);
        if (_nullable) {
            auto logical_field = logical_field_or_fallback(ctx, binding_key, _field);
            return make_nullable_scorer(scorer, logical_field, ctx.null_resolver);
        }
        return scorer;
    }

private:
    ScorerPtr phrase_scorer(const QueryExecutionContext& ctx, const std::string& binding_key) {
        auto reader = lookup_reader(_field, ctx, binding_key);
        if (!reader) {
            throw Exception(ErrorCode::NOT_FOUND, "Reader not found for field '{}'",
                            StringHelper::to_string(_field));
        }

        std::vector<std::pair<size_t, SegmentPostingsPtr>> term_postings_list;
        for (const auto& term_info : _term_infos) {
            size_t offset = term_info.position;
            auto posting =
                    create_position_posting(reader.get(), _field, term_info.get_single_term(),
                                            _enable_scoring, _context->io_ctx);
            if (posting) {
                term_postings_list.emplace_back(offset, std::move(posting));
            } else {
                return std::make_shared<EmptyScorer>();
            }
        }
        uint32_t num_docs = ctx.segment_num_rows;
        return PhraseScorer<SegmentPostingsPtr>::create(term_postings_list, _similarity, 0,
                                                        num_docs);
    }

    IndexQueryContextPtr _context;

    std::wstring _field;
    std::vector<TermInfo> _term_infos;
    SimilarityPtr _similarity;
    bool _enable_scoring = false;
    bool _nullable = true;
};

} // namespace doris::segment_v2::inverted_index::query_v2
