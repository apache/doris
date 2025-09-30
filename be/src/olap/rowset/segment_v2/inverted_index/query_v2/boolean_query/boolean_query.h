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

#include "olap/rowset/segment_v2/inverted_index/query_v2/boolean_query/boolean_weight.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/operator.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/query.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/score_combiner.h"

namespace doris::segment_v2::inverted_index::query_v2 {

class BooleanQuery;
using BooleanQueryPtr = std::shared_ptr<BooleanQuery>;

class BooleanQuery : public Query {
public:
    BooleanQuery(OperatorType type, std::vector<QueryPtr> clauses,
                 std::vector<std::string> binding_keys)
            : _type(type),
              _sub_queries(std::move(clauses)),
              _binding_keys(std::move(binding_keys)) {}
    ~BooleanQuery() override = default;

    WeightPtr weight(bool enable_scoring) override {
        std::vector<WeightPtr> sub_weights;
        sub_weights.reserve(_sub_queries.size());
        for (const auto& query : _sub_queries) {
            sub_weights.emplace_back(query->weight(enable_scoring));
        }
        if (enable_scoring) {
            return std::make_shared<BooleanWeight<SumCombinerPtr>>(
                    _type, std::move(sub_weights), _binding_keys, std::make_shared<SumCombiner>());
        } else {
            return std::make_shared<BooleanWeight<DoNothingCombinerPtr>>(
                    _type, std::move(sub_weights), _binding_keys,
                    std::make_shared<DoNothingCombiner>());
        }
    }

    class Builder {
    public:
        Builder(OperatorType type) : _type(type) {}
        ~Builder() = default;

        void add(const QueryPtr& query, std::string binding_key = {}) {
            _sub_queries.emplace_back(query);
            _binding_keys.emplace_back(std::move(binding_key));
        }

        BooleanQueryPtr build() {
            return std::make_shared<BooleanQuery>(_type, std::move(_sub_queries),
                                                  std::move(_binding_keys));
        }

    private:
        OperatorType _type;
        std::vector<QueryPtr> _sub_queries;
        std::vector<std::string> _binding_keys;
    };

private:
    OperatorType _type;
    std::vector<QueryPtr> _sub_queries;
    std::vector<std::string> _binding_keys;
};

} // namespace doris::segment_v2::inverted_index::query_v2
