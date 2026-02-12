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
#include "olap/rowset/segment_v2/inverted_index/query_v2/prefix_query/prefix_weight.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/scorer.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/union_postings.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/weight.h"
#include "olap/rowset/segment_v2/inverted_index/util/string_helper.h"

namespace doris::segment_v2::inverted_index::query_v2 {

class PhrasePrefixWeight : public Weight {
public:
    PhrasePrefixWeight(IndexQueryContextPtr context, std::wstring field,
                       std::vector<std::pair<size_t, std::string>> phrase_terms,
                       std::pair<size_t, std::string> prefix, SimilarityPtr similarity,
                       bool enable_scoring, int32_t max_expansions, bool nullable)
            : _context(std::move(context)),
              _field(std::move(field)),
              _phrase_terms(std::move(phrase_terms)),
              _prefix(std::move(prefix)),
              _similarity(std::move(similarity)),
              _enable_scoring(enable_scoring),
              _max_expansions(max_expansions),
              _nullable(nullable) {}
    ~PhrasePrefixWeight() override = default;

    ScorerPtr scorer(const QueryExecutionContext& ctx, const std::string& binding_key) override {
        auto scorer = phrase_prefix_scorer(ctx, binding_key);
        if (_nullable) {
            auto logical_field = logical_field_or_fallback(ctx, binding_key, _field);
            return make_nullable_scorer(scorer, logical_field, ctx.null_resolver);
        }
        return scorer;
    }

private:
    ScorerPtr phrase_prefix_scorer(const QueryExecutionContext& ctx,
                                   const std::string& binding_key) {
        auto reader = lookup_reader(_field, ctx, binding_key);
        if (!reader) {
            throw Exception(ErrorCode::NOT_FOUND, "Reader not found for field '{}'",
                            StringHelper::to_string(_field));
        }

        std::vector<std::pair<size_t, PostingsPtr>> all_postings;
        for (const auto& [offset, term] : _phrase_terms) {
            auto posting = create_position_posting(reader.get(), _field, term, _enable_scoring,
                                                   _context->io_ctx);
            if (!posting) {
                return std::make_shared<EmptyScorer>();
            }
            all_postings.emplace_back(offset, std::move(posting));
        }

        auto expanded_terms = PrefixWeight::expand_prefix(reader.get(), _field, _prefix.second,
                                                          _max_expansions, _context->io_ctx);
        if (expanded_terms.empty()) {
            return std::make_shared<EmptyScorer>();
        }

        std::vector<SegmentPostingsPtr> suffix_postings;
        for (const auto& term : expanded_terms) {
            auto posting = create_position_posting(reader.get(), _field, term, _enable_scoring,
                                                   _context->io_ctx);
            if (posting) {
                suffix_postings.emplace_back(std::move(posting));
            }
        }

        if (suffix_postings.empty()) {
            return std::make_shared<EmptyScorer>();
        }

        all_postings.emplace_back(_prefix.first, make_union_postings(std::move(suffix_postings)));

        uint32_t num_docs = ctx.segment_num_rows;
        return PhraseScorer<PostingsPtr>::create(all_postings, _similarity, 0, num_docs);
    }

    IndexQueryContextPtr _context;
    std::wstring _field;
    std::vector<std::pair<size_t, std::string>> _phrase_terms;
    std::pair<size_t, std::string> _prefix;
    SimilarityPtr _similarity;
    bool _enable_scoring = false;
    int32_t _max_expansions = 50;
    bool _nullable = true;
};

} // namespace doris::segment_v2::inverted_index::query_v2
