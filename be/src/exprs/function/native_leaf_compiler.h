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

#include "exprs/function/search_leaf_compiler.h"
#include "storage/index/inverted/inverted_index_reader.h"

namespace doris {

// Answers a leaf through the analyzed-query API of an index reader that runs
// queries itself (SNII today) and wraps the rows it matched, their null bitmap
// and any scores it published as a bit-set query.
class NativeLeafCompiler final : public SearchLeafCompiler {
public:
    NativeLeafCompiler(segment_v2::InvertedIndexReaderPtr reader, std::string stored_field_name);

    Status compile(const index_query::logical::Node& leaf, const SearchLeafContext& ctx,
                   segment_v2::inverted_index::query_v2::QueryPtr* out) override;

private:
    segment_v2::InvertedIndexReaderPtr _reader;
    std::string _stored_field_name;
};

} // namespace doris
