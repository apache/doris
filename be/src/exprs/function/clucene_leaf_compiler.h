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

namespace doris {

// Compiles a leaf into the lazy query_v2 query the CLucene engine evaluates.
class CluceneLeafCompiler final : public SearchLeafCompiler {
public:
    CluceneLeafCompiler(std::wstring field, std::string binding_key);

    Status compile(const index_query::logical::Node& leaf, const SearchLeafContext& ctx,
                   segment_v2::inverted_index::query_v2::QueryPtr* out) override;

private:
    std::wstring _field;
    std::string _binding_key;
};

} // namespace doris
