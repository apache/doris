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

#include "exprs/function/clucene_leaf_compiler.h"

#include <utility>

#include "storage/index/inverted/inverted_index_reader.h"

namespace doris {

CluceneLeafCompiler::CluceneLeafCompiler(std::wstring field, std::string binding_key)
        : _field(std::move(field)), _binding_key(std::move(binding_key)) {}

Status CluceneLeafCompiler::compile(const index_query::logical::Node& leaf,
                                    const SearchLeafContext& ctx,
                                    segment_v2::inverted_index::query_v2::QueryPtr* out) {
    return segment_v2::plan_clucene_query(leaf, ctx.context, _field, _binding_key,
                                          /*candidates=*/nullptr, out);
}

} // namespace doris
