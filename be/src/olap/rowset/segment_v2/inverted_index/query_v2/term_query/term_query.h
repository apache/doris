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

#include "olap/rowset/segment_v2/inverted_index/query_v2/query.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/term_query/term_weight.h"
#include "olap/rowset/segment_v2/inverted_index/similarity/bm25_similarity.h"

namespace doris::segment_v2::inverted_index::query_v2 {

class TermQuery : public Query {
public:
    TermQuery(IndexQueryContextPtr context, std::wstring field, std::wstring term)
            : _context(std::move(context)), _field(std::move(field)), _term(std::move(term)) {}
    ~TermQuery() override = default;

    WeightPtr weight(bool enable_scoring) override {
        SimilarityPtr bm25_similarity;
        if (enable_scoring) {
            bm25_similarity = std::make_shared<BM25Similarity>();
            bm25_similarity->for_one_term(_context, _field, _term);
        } else {
            bm25_similarity = std::make_shared<BM25Similarity>(1.0F, 1.0F);
        }
        return std::make_shared<TermWeight>(std::move(_context), std::move(_field),
                                            std::move(_term), std::move(bm25_similarity),
                                            enable_scoring);
    }

private:
    IndexQueryContextPtr _context;

    std::wstring _field;
    std::wstring _term;
};

} // namespace doris::segment_v2::inverted_index::query_v2