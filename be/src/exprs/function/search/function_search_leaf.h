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
#include <string>
#include <utility>

#include "common/status.h"
#include "core/data_type/data_type.h"
#include "storage/index/index_iterator.h"
#include "storage/index/index_query_context.h"
#include "storage/index/inverted/query/engine/bit_set_query/bit_set_query.h"
#include "storage/index/inverted/query/engine/query.h"
#include "storage/index/inverted/query/logical/node.h"
#include "storage/index/inverted/query/spi/index_source.h"

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

// Compiles a leaf into the lazy query_v2 query the engine evaluates on the field's bound source,
// after the source resolved the leaf's terms in one dictionary batch.
class LazyLeafCompiler final : public SearchLeafCompiler {
public:
    LazyLeafCompiler(std::wstring field, std::string binding_key,
                     index_query::IndexSourcePtr source);

    Status compile(const index_query::logical::Node& leaf, const SearchLeafContext& ctx,
                   segment_v2::inverted_index::query_v2::QueryPtr* out) override;

private:
    std::wstring _field;
    std::string _binding_key;
    index_query::IndexSourcePtr _source;
};

// Answers a comparison leaf from a scalar (BKD) index through its iterator.
// Every other leaf on such an index is UNKNOWN.
class ScalarLeafCompiler final : public SearchLeafCompiler {
public:
    ScalarLeafCompiler(segment_v2::IndexIterator* iterator, DataTypePtr column_type,
                       std::string stored_field_name);

    Status compile(const index_query::logical::Node& leaf, const SearchLeafContext& ctx,
                   segment_v2::inverted_index::query_v2::QueryPtr* out) override;

private:
    segment_v2::IndexIterator* _iterator;
    DataTypePtr _column_type;
    std::string _stored_field_name;
};

} // namespace doris
