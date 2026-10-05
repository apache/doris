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

#include <cstdint>
#include <memory>
#include <roaring/roaring.hh>

#include "common/status.h"
#include "storage/index/index_query_context.h"
#include "storage/index/inverted/query_v2/bit_set_query/bit_set_query.h"
#include "storage/index/inverted/query_v2/query.h"
#include "storage/index/query/logical/node.h"

namespace doris {

// What a leaf needs from the query it is part of.
struct SearchLeafContext {
    std::shared_ptr<segment_v2::IndexQueryContext> context;
    uint32_t num_rows = 0;
    // Whether the query scores its rows; clauses that only add to the score matter only then.
    bool scoring = true;
};

// Compiles one lowered leaf on its bound text or scalar index.
class SearchLeafCompiler {
public:
    virtual ~SearchLeafCompiler() = default;

    virtual Status compile(const index_query::logical::Node& leaf, const SearchLeafContext& ctx,
                           segment_v2::inverted_index::query_v2::QueryPtr* out) = 0;
};

// UNKNOWN for every row: no row matches and every row counts as NULL.
inline segment_v2::inverted_index::query_v2::QueryPtr make_unknown_leaf_query(uint32_t num_rows) {
    auto null_bitmap = std::make_shared<roaring::Roaring>();
    if (num_rows > 0) {
        null_bitmap->addRange(0, num_rows);
    }
    return std::make_shared<segment_v2::inverted_index::query_v2::BitSetQuery>(
            std::make_shared<roaring::Roaring>(), std::move(null_bitmap));
}

} // namespace doris
