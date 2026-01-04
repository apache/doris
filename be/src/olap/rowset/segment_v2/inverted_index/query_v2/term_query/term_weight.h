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
               SimilarityPtr similarity, bool enable_scoring)
            : _context(std::move(context)),
              _field(std::move(field)),
              _term(std::move(term)),
              _similarity(std::move(similarity)),
              _enable_scoring(enable_scoring) {}
    ~TermWeight() override = default;

    ScorerPtr scorer(const QueryExecutionContext& ctx, const std::string& binding_key) override {
        auto reader = lookup_reader(_field, ctx, binding_key);
        auto logical_field = logical_field_or_fallback(ctx, binding_key, _field);

        if (!reader) {
            return std::make_shared<EmptyScorer>();
        }

        auto t = make_term_ptr(_field.c_str(), _term.c_str());
        auto iter = make_term_doc_ptr(reader.get(), t.get(), _enable_scoring, _context->io_ctx);
        if (iter) {
            return std::make_shared<TermScorer>(
                    make_segment_postings(std::move(iter), _enable_scoring), _similarity,
                    logical_field);
        }

        return std::make_shared<EmptyScorer>();
    }

private:
    IndexQueryContextPtr _context;

    std::wstring _field;
    std::wstring _term;
    SimilarityPtr _similarity;
    bool _enable_scoring = false;
};

} // namespace doris::segment_v2::inverted_index::query_v2
