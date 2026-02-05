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

#include <vector>

#include "olap/rowset/segment_v2/inverted_index/query_v2/boolean_query/occur.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/boolean_query/occur_boolean_query.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/boolean_query/operator_boolean_query.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/query.h"

namespace doris::segment_v2::inverted_index::query_v2 {

class OccurBooleanQueryBuilder {
public:
    OccurBooleanQueryBuilder() = default;
    ~OccurBooleanQueryBuilder() = default;

    void add(const QueryPtr& query, Occur occur) { _sub_queries.emplace_back(occur, query); }

    void set_minimum_number_should_match(size_t value) { _minimum_number_should_match = value; }

    QueryPtr build() {
        if (_minimum_number_should_match.has_value()) {
            return std::make_shared<OccurBooleanQuery>(std::move(_sub_queries),
                                                       _minimum_number_should_match.value());
        }
        return std::make_shared<OccurBooleanQuery>(std::move(_sub_queries));
    }

private:
    std::vector<std::pair<Occur, QueryPtr>> _sub_queries;
    std::optional<size_t> _minimum_number_should_match;
};

using OccurBooleanQueryBuilderPtr = std::shared_ptr<OccurBooleanQueryBuilder>;

class OperatorBooleanQueryBuilder {
public:
    OperatorBooleanQueryBuilder(OperatorType type) : _type(type) {}
    ~OperatorBooleanQueryBuilder() = default;

    void add(const QueryPtr& query, std::string binding_key = {}) {
        _sub_queries.emplace_back(query);
        _binding_keys.emplace_back(std::move(binding_key));
    }

    QueryPtr build() {
        return std::make_shared<OperatorBooleanQuery>(_type, std::move(_sub_queries),
                                                      std::move(_binding_keys));
    }

private:
    OperatorType _type;
    std::vector<QueryPtr> _sub_queries;
    std::vector<std::string> _binding_keys;
};

using OperatorBooleanQueryBuilderPtr = std::shared_ptr<OperatorBooleanQueryBuilder>;

inline OccurBooleanQueryBuilderPtr create_occur_boolean_query_builder() {
    return std::make_shared<OccurBooleanQueryBuilder>();
}

inline OperatorBooleanQueryBuilderPtr create_operator_boolean_query_builder(OperatorType type) {
    return std::make_shared<OperatorBooleanQueryBuilder>(type);
}

} // namespace doris::segment_v2::inverted_index::query_v2
