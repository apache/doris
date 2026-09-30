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

#include <string>
#include <variant>

#include "storage/index/inverted/query_v2/segment_postings.h"
#include "storage/index/inverted/query_v2/term_query/term_scorer.h"
#include "storage/index/inverted/query_v2/wand/block_wand.h"
#include "storage/index/inverted/query_v2/weight.h"
#include "storage/index/query/spi/scoring_context.h"

namespace doris::segment_v2::inverted_index::query_v2 {

using TermOrEmptyScorer = std::variant<EmptyScorerPtr, TermScorerPtr>;

// One UTF-8 term on one field.
class TermWeight : public Weight {
public:
    using Weight::for_each_pruning;

    TermWeight(std::wstring field, std::string term,
               index_query::ScoringContextPtr<float> similarity, bool enable_scoring)
            : _field(std::move(field)),
              _term(std::move(term)),
              _similarity(std::move(similarity)),
              _enable_scoring(enable_scoring) {}
    ~TermWeight() override = default;

    ScorerPtr scorer(const QueryExecutionContext& ctx, const std::string& binding_key) override {
        auto result = specialized_scorer(ctx, binding_key);
        return std::visit([](auto&& sc) -> ScorerPtr { return sc; }, result);
    }

    template <typename Callback>
    void for_each_pruning(const QueryExecutionContext& context, const std::string& binding_key,
                          float threshold, Callback&& callback) {
        auto result = specialized_scorer(context, binding_key);
        std::visit(
                [&](auto&& sc) {
                    using T = std::decay_t<decltype(sc)>;
                    if constexpr (std::is_same_v<T, TermScorerPtr>) {
                        block_wand_single_scorer(std::move(sc), threshold,
                                                 std::forward<Callback>(callback));
                    }
                },
                std::move(result));
    }

    const std::wstring& field() const { return _field; }
    const std::string& term() const { return _term; }
    const index_query::ScoringContextPtr<float>& similarity() const { return _similarity; }
    bool scores() const { return _enable_scoring; }

private:
    TermOrEmptyScorer specialized_scorer(const QueryExecutionContext& ctx,
                                         const std::string& binding_key) {
        auto source = lookup_source(_field, ctx, binding_key);
        auto logical_field = logical_field_or_fallback(ctx, binding_key, _field);
        if (!source) {
            return std::make_shared<EmptyScorer>();
        }

        SegmentPostingsPtr segment_postings =
                open_postings(*source, _term, /*positions=*/false, _enable_scoring, _similarity);
        if (segment_postings) {
            return std::make_shared<TermScorer>(segment_postings, _similarity, logical_field);
        }
        return std::make_shared<EmptyScorer>();
    }

    std::wstring _field;
    std::string _term;
    index_query::ScoringContextPtr<float> _similarity;
    bool _enable_scoring = false;
};

} // namespace doris::segment_v2::inverted_index::query_v2
