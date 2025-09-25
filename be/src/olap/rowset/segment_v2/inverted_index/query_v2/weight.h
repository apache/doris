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

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "olap/rowset/segment_v2/inverted_index/query_v2/scorer.h"

namespace lucene::index {
class IndexReader;
}

namespace doris::segment_v2::inverted_index::query_v2 {

struct QueryExecutionContext {
    uint32_t segment_num_rows = 0;
    std::vector<std::shared_ptr<lucene::index::IndexReader>> readers;
    std::unordered_map<std::string, std::shared_ptr<lucene::index::IndexReader>> reader_bindings;
    std::unordered_map<std::wstring, std::shared_ptr<lucene::index::IndexReader>>
            field_reader_bindings;
};

class Weight {
public:
    Weight() = default;
    virtual ~Weight() = default;

    virtual ScorerPtr scorer(const QueryExecutionContext& context) = 0;
    virtual ScorerPtr scorer(const QueryExecutionContext& context, const std::string& binding_key) {
        (void)binding_key;
        return scorer(context);
    }
};
using WeightPtr = std::shared_ptr<Weight>;

} // namespace doris::segment_v2::inverted_index::query_v2
