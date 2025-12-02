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

#include "olap/rowset/segment_v2/inverted_index/query_v2/boolean_query/occur.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/boolean_query/occur_boolean_weight.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/query.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/score_combiner.h"

namespace doris::segment_v2::inverted_index::query_v2 {

class OccurBooleanQuery;
using OccurBooleanQueryPtr = std::shared_ptr<OccurBooleanQuery>;

class OccurBooleanQuery : public Query {
public:
    OccurBooleanQuery(std::vector<std::pair<Occur, QueryPtr>> clauses)
            : _sub_queries(std::move(clauses)) {}
    ~OccurBooleanQuery() override = default;

    WeightPtr weight(bool enable_scoring) override {
        std::vector<std::pair<Occur, WeightPtr>> sub_weights;
        sub_weights.reserve(_sub_queries.size());
        for (const auto& [occur, query] : _sub_queries) {
            sub_weights.emplace_back(occur, query->weight(enable_scoring));
        }
        return std::make_shared<OccurBooleanWeight<SumCombinerPtr>>(
                std::move(sub_weights), minimum_number_should_match, enable_scoring,
                std::make_shared<SumCombiner>());
    }

    const std::vector<std::pair<Occur, QueryPtr>>& clauses() const { return _sub_queries; }

private:
    std::vector<std::pair<Occur, QueryPtr>> _sub_queries;
    size_t minimum_number_should_match = 1;
};

} // namespace doris::segment_v2::inverted_index::query_v2