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

#include "exprs/function/lazy_leaf_compiler.h"
#include "exprs/function/search_leaf_compiler.h"
#include "storage/index/inverted/inverted_index_reader.h"

namespace doris {

// Compiles the leaves of an index that still scores itself (SNII): a scored term or phrase leaf
// runs through the reader, which publishes its BM25 values, and becomes a scored bit-set query;
// every other leaf compiles lazily on the bound source, like any index's.
class NativeLeafCompiler final : public SearchLeafCompiler {
public:
    NativeLeafCompiler(segment_v2::InvertedIndexReaderPtr reader, std::string stored_field_name,
                       std::shared_ptr<LazyLeafCompiler> lazy);

    Status compile(const index_query::logical::Node& leaf, const SearchLeafContext& ctx,
                   segment_v2::inverted_index::query_v2::QueryPtr* out) override;

    bool joins_term_sets() const override { return true; }

private:
    Status _compile_scored(const index_query::logical::Node& leaf, const SearchLeafContext& ctx,
                           segment_v2::inverted_index::query_v2::QueryPtr* out);

    segment_v2::InvertedIndexReaderPtr _reader;
    std::string _stored_field_name;
    std::shared_ptr<LazyLeafCompiler> _lazy;
};

} // namespace doris
