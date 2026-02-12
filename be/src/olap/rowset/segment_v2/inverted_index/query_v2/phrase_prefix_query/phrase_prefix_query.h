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

#include "common/exception.h"
#include "olap/rowset/segment_v2/index_query_context.h"
#include "olap/rowset/segment_v2/inverted_index/query/query_info.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/phrase_prefix_query/phrase_prefix_weight.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/prefix_query/prefix_query.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/query.h"
#include "olap/rowset/segment_v2/inverted_index/similarity/bm25_similarity.h"

namespace doris::segment_v2::inverted_index::query_v2 {

class PhrasePrefixQuery : public Query {
public:
    PhrasePrefixQuery(IndexQueryContextPtr context, std::wstring field,
                      const std::vector<TermInfo>& terms)
            : _context(std::move(context)), _field(std::move(field)) {
        std::vector<std::pair<size_t, std::string>> terms_with_offset;
        for (size_t i = 0; i < terms.size(); ++i) {
            terms_with_offset.emplace_back(i, terms[i].get_single_term());
        }
        assert(!terms.empty());
        _prefix = std::move(terms_with_offset.back());
        terms_with_offset.pop_back();
        _phrase_terms = std::move(terms_with_offset);
    }

    ~PhrasePrefixQuery() override = default;

    WeightPtr weight(bool enable_scoring) override {
        if (!_prefix.has_value()) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "PhrasePrefixQuery requires a prefix term");
        }

        auto weight = phrase_prefix_query_weight(enable_scoring);
        if (weight) {
            return weight;
        }

        // Only prefix term, no phrase terms — fall back to a plain prefix query.
        PrefixQuery prefix_query(_context, std::move(_field), std::move(_prefix.value().second));
        return prefix_query.weight(enable_scoring);
    }

private:
    WeightPtr phrase_prefix_query_weight(bool enable_scoring) {
        if (_phrase_terms.empty()) {
            return nullptr;
        }

        SimilarityPtr bm25_similarity;
        if (enable_scoring) {
            bm25_similarity = std::make_shared<BM25Similarity>();
            std::vector<std::wstring> all_terms;
            for (const auto& phrase_term : _phrase_terms) {
                all_terms.push_back(StringHelper::to_wstring(phrase_term.second));
            }
            bm25_similarity->for_terms(_context, _field, all_terms);
        }

        return std::make_shared<PhrasePrefixWeight>(
                _context, std::move(_field), std::move(_phrase_terms), std::move(_prefix.value()),
                std::move(bm25_similarity), enable_scoring, _max_expansions, _nullable);
    }

    IndexQueryContextPtr _context;
    std::wstring _field;
    std::vector<std::pair<size_t, std::string>> _phrase_terms;
    std::optional<std::pair<size_t, std::string>> _prefix;
    int32_t _max_expansions = 50;
    bool _nullable = true;
};

} // namespace doris::segment_v2::inverted_index::query_v2
