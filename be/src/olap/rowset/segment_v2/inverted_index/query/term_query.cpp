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

#include "term_query.h"

#include "olap/rowset/segment_v2/inverted_index/query/query_helper.h"

namespace doris::segment_v2 {

TermQuery::TermQuery(SearcherPtr searcher, IndexQueryContextPtr context)
        : _searcher(std::move(searcher)), _context(std::move(context)) {}

void TermQuery::add(const InvertedIndexQueryInfo& query_info) {
    if (query_info.term_infos.size() != 1) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "term_infos size must be 1");
    }

    _iter = TermIterator::create(_context->io_ctx, _searcher->getReader(), query_info.field_name,
                                 query_info.term_infos[0].get_single_term());

    if (_context->collection_similarity && query_info.is_similarity_score) {
        _similaritie = std::make_unique<BM25Similarity>();
        _similaritie->for_one_term(_context, query_info.field_name, _iter->term());
    }
}

void TermQuery::search(roaring::Roaring& roaring) {
    DocRange doc_range;
    while (_iter->read_range(&doc_range)) {
        if (doc_range.type_ == DocRangeType::kMany) {
            roaring.addMany(doc_range.doc_many_size_, doc_range.doc_many->data());

            if (_similaritie) {
                QueryHelper::collect_many(_context, _similaritie, doc_range);
            }
        } else {
            roaring.addRange(doc_range.doc_range.first, doc_range.doc_range.second);

            if (_similaritie) {
                QueryHelper::collect_range(_context, _similaritie, doc_range);
            }
        }
    }
}

} // namespace doris::segment_v2