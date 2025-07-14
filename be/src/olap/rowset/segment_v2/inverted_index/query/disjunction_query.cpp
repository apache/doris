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

#include "disjunction_query.h"

namespace doris::segment_v2 {

DisjunctionQuery::DisjunctionQuery(const std::shared_ptr<lucene::search::IndexSearcher>& searcher,
                                   const TQueryOptions& query_options, const io::IOContext* io_ctx)
        : _searcher(searcher), _io_ctx(io_ctx) {}

void DisjunctionQuery::add(const InvertedIndexQueryInfo& query_info) {
    if (query_info.term_infos.empty()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "term_infos cannot be empty");
    }

    _field_name = query_info.field_name;
    _term_infos = query_info.term_infos;
}

void DisjunctionQuery::search(roaring::Roaring& roaring) {
    auto func = [this, &roaring](const std::string& term, bool first) {
        auto iter = TermIterator::create(_io_ctx, _searcher->getReader(), _field_name, term);

        DocRange doc_range;
        roaring::Roaring result;
        while (iter->read_range(&doc_range)) {
            if (doc_range.type_ == DocRangeType::kMany) {
                result.addMany(doc_range.doc_many_size_, doc_range.doc_many->data());
            } else {
                result.addRange(doc_range.doc_range.first, doc_range.doc_range.second);
            }
        }

        if (first) {
            roaring.swap(result);
        } else {
            roaring |= result;
        }
    };
    for (size_t i = 0; i < _term_infos.size(); i++) {
        const auto& term_info = _term_infos[i];
        if (term_info.is_single_term()) {
            func(term_info.get_single_term(), i == 0);
        } else {
            const auto& terms = term_info.get_multi_terms();
            for (size_t j = 0; j < terms.size(); j++) {
                func(terms[j], (i == 0 && j == 0));
            }
        }
    }
}

} // namespace doris::segment_v2