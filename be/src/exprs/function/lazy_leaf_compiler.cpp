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

#include "exprs/function/lazy_leaf_compiler.h"

#include <vector>

#include "storage/index/inverted/inverted_index_reader.h"
#include "storage/index/query/logical/node.h"

namespace doris {

namespace {

namespace logical = index_query::logical;

// The terms a leaf opens, for the source to resolve ahead in one batch.
std::vector<std::string> leaf_terms(const logical::Node& leaf) {
    std::vector<std::string> terms;
    if (const auto* term = leaf.as<logical::Term>()) {
        terms.push_back(term->term);
    } else if (const auto* set = leaf.as<logical::TermSet>()) {
        terms = set->terms;
    } else if (const auto* phrase = leaf.as<logical::Phrase>()) {
        for (const auto& slot : phrase->slots) {
            if (slot.is_single_term()) {
                terms.push_back(slot.get_single_term());
            } else {
                const auto& alternatives = slot.get_multi_terms();
                terms.insert(terms.end(), alternatives.begin(), alternatives.end());
            }
        }
    }
    return terms;
}

} // namespace

LazyLeafCompiler::LazyLeafCompiler(std::wstring field, std::string binding_key,
                                   index_query::IndexSourcePtr source)
        : _field(std::move(field)),
          _binding_key(std::move(binding_key)),
          _source(std::move(source)) {}

Status LazyLeafCompiler::compile(const logical::Node& leaf, const SearchLeafContext& ctx,
                                 segment_v2::inverted_index::query_v2::QueryPtr* out) {
    const std::vector<std::string> terms = leaf_terms(leaf);
    if (!terms.empty()) {
        RETURN_IF_ERROR(_source->prepare_terms(terms));
    }
    return segment_v2::plan_query(leaf, ctx.context, _field, _binding_key, /*candidates=*/nullptr,
                                  out);
}

} // namespace doris
