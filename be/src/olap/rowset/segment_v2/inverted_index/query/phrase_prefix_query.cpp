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

#include "phrase_prefix_query.h"

#include "olap/rowset/segment_v2/inverted_index/query/query.h"

namespace doris::segment_v2 {
#include "common/compile_check_begin.h"

PhrasePrefixQuery::PhrasePrefixQuery(const std::shared_ptr<lucene::search::IndexSearcher>& searcher,
                                     const TQueryOptions& query_options,
                                     const io::IOContext* io_ctx)
        : _searcher(searcher),
          _max_expansions(query_options.inverted_index_max_expansions),
          _phrase_query(searcher, query_options, io_ctx),
          _prefix_query(searcher, query_options, io_ctx) {}

void PhrasePrefixQuery::add(const InvertedIndexQueryInfo& query_info) {
    if (query_info.term_infos.empty()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "term_infos cannot be empty");
    }

    _term_size = query_info.term_infos.size();

    InvertedIndexQueryInfo new_query_info;
    new_query_info.field_name = query_info.field_name;
    new_query_info.term_infos.resize(query_info.term_infos.size());
    for (size_t i = 0; i < query_info.term_infos.size(); i++) {
        const auto& term_info = query_info.term_infos[i];
        if (term_info.is_multi_terms()) {
            throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR, "Not supported yet.");
        }

        if (i < query_info.term_infos.size() - 1) {
            new_query_info.term_infos[i].term = query_info.term_infos[i].get_single_term();
            new_query_info.term_infos[i].position = query_info.term_infos[i].position;
        } else {
            std::vector<std::string> prefix_terms;
            _prefix_query.get_prefix_terms(_searcher->getReader(), query_info.field_name,
                                           query_info.term_infos[i].get_single_term(), prefix_terms,
                                           _max_expansions);
            if (prefix_terms.empty()) {
                prefix_terms.emplace_back(query_info.term_infos[i].get_single_term());
            }
            new_query_info.term_infos[i].term = std::move(prefix_terms);
            new_query_info.term_infos[i].position = query_info.term_infos[i].position;
        }
    }

    if (_term_size == 1) {
        _prefix_query.add(new_query_info);
    } else {
        _phrase_query.add(new_query_info);
    }
}

void PhrasePrefixQuery::search(roaring::Roaring& roaring) {
    if (_term_size == 1) {
        _prefix_query.search(roaring);
    } else {
        _phrase_query.search(roaring);
    }
}

#include "common/compile_check_end.h"
} // namespace doris::segment_v2