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

#include "olap/rowset/segment_v2/inverted_index/query_v2/segment_postings.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/term_query/term_scorer.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/weight.h"
#include "olap/rowset/segment_v2/inverted_index/similarity/similarity.h"

namespace doris::segment_v2::inverted_index::query_v2 {

class TermWeight : public Weight {
public:
    TermWeight(IndexQueryContextPtr context, std::wstring field, std::wstring term,
               SimilarityPtr similarity, bool enable_scoring, std::string logical_field = {})
            : _context(std::move(context)),
              _field(std::move(field)),
              _term(std::move(term)),
              _similarity(std::move(similarity)),
              _enable_scoring(enable_scoring),
              _logical_field(std::move(logical_field)) {}
    ~TermWeight() override = default;

    ScorerPtr scorer(const QueryExecutionContext& ctx, const std::string& binding_key) override {
        auto reader = lookup_reader(_field, ctx, binding_key);
        auto field_name =
                _logical_field.empty() ? std::string(_field.begin(), _field.end()) : _logical_field;
        auto make_scorer = [&](auto segment_postings) -> ScorerPtr {
            using PostingsT = decltype(segment_postings);
            return std::make_shared<TermScorer<PostingsT>>(std::move(segment_postings), _similarity,
                                                           field_name);
        };

        if (!reader) {
            auto segment_postings = std::make_shared<EmptySegmentPosting<TermDocsPtr>>();
            return make_scorer(std::move(segment_postings));
        }

        auto t = make_term_ptr(_field.c_str(), _term.c_str());
        auto iter = make_term_doc_ptr(reader.get(), t.get(), _enable_scoring, _context->io_ctx);

        if (iter) {
            if (_enable_scoring) {
                auto segment_postings =
                        std::make_shared<SegmentPostings<TermDocsPtr>>(std::move(iter));
                return make_scorer(std::move(segment_postings));
            }
            auto segment_postings =
                    std::make_shared<NoScoreSegmentPosting<TermDocsPtr>>(std::move(iter));
            return make_scorer(std::move(segment_postings));
        }

        auto segment_postings = std::make_shared<EmptySegmentPosting<TermDocsPtr>>();
        return make_scorer(std::move(segment_postings));
    }

private:
    IndexQueryContextPtr _context;

    std::wstring _field;
    std::wstring _term;
    SimilarityPtr _similarity;
    bool _enable_scoring = false;
    std::string _logical_field;
};

} // namespace doris::segment_v2::inverted_index::query_v2
