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

#include <memory>
#include <roaring/roaring.hh>
#include <utility>

#include "storage/index/inverted/query/query_helper.h"
#include "storage/index/inverted/query_v2/all_query/all_query.h"
#include "storage/index/inverted/query_v2/boolean_query/boolean_query_builder.h"
#include "storage/index/inverted/query_v2/boolean_query/operator.h"
#include "storage/index/inverted/query_v2/phrase_query/multi_phrase_query.h"
#include "storage/index/inverted/query_v2/phrase_query/phrase_query.h"
#include "storage/index/inverted/query_v2/regexp_query/regexp_query.h"
#include "storage/index/inverted/query_v2/term_query/term_query.h"
#include "storage/index/inverted/query_v2/wildcard_query/wildcard_query.h"
#include "storage/index/inverted/util/string_helper.h"

namespace doris {
namespace {

namespace logical = index_query::logical;
namespace query_v2 = segment_v2::inverted_index::query_v2;

query_v2::QueryPtr term_query(const SearchLeafContext& ctx, const std::wstring& field,
                              const std::string& term) {
    return std::make_shared<query_v2::TermQuery>(ctx.context, field,
                                                 segment_v2::StringHelper::to_wstring(term));
}

// One term is queried as itself; several form the boolean the clause asked for.
// The SEARCH compile step counts a threshold before a set reaches this compiler.
query_v2::QueryPtr term_set_query(const SearchLeafContext& ctx, const std::wstring& field,
                                  const std::string& binding_key, const logical::TermSet& set) {
    DORIS_CHECK(set.min_should_match == 0);
    if (set.terms.size() == 1) {
        return term_query(ctx, field, set.terms.front());
    }
    auto builder = query_v2::create_operator_boolean_query_builder(
            set.require_all ? query_v2::OperatorType::OP_AND : query_v2::OperatorType::OP_OR);
    for (const auto& term : set.terms) {
        builder->add(term_query(ctx, field, term), binding_key);
    }
    return builder->build();
}

query_v2::QueryPtr phrase_query(const SearchLeafContext& ctx, const std::wstring& field,
                                const logical::Phrase& phrase) {
    if (segment_v2::QueryHelper::is_simple_phrase(phrase.slots)) {
        return std::make_shared<query_v2::PhraseQuery>(ctx.context, field, phrase.slots);
    }
    return std::make_shared<query_v2::MultiPhraseQuery>(ctx.context, field, phrase.slots);
}

query_v2::QueryPtr pattern_query(const SearchLeafContext& ctx, const std::wstring& field,
                                 logical::ExpandKind kind, const std::string& pattern) {
    if (kind == logical::ExpandKind::kRegexp) {
        return std::make_shared<query_v2::RegexpQuery>(ctx.context, field, pattern);
    }
    return std::make_shared<query_v2::WildcardQuery>(ctx.context, field, pattern);
}

} // namespace

CluceneLeafCompiler::CluceneLeafCompiler(std::wstring field, std::string binding_key)
        : _field(std::move(field)), _binding_key(std::move(binding_key)) {}

Status CluceneLeafCompiler::compile(const logical::Node& leaf, const SearchLeafContext& ctx,
                                    query_v2::QueryPtr* out) {
    if (const auto* term = leaf.as<logical::Term>()) {
        *out = term_query(ctx, _field, term->term);
    } else if (const auto* set = leaf.as<logical::TermSet>()) {
        *out = term_set_query(ctx, _field, _binding_key, *set);
    } else if (const auto* phrase = leaf.as<logical::Phrase>()) {
        *out = phrase_query(ctx, _field, *phrase);
    } else if (const auto* prefix = leaf.as<logical::Prefix>()) {
        // The whole normalized value, trailing '*' included, is one wildcard.
        *out = pattern_query(ctx, _field, logical::ExpandKind::kWildcard, prefix->pattern);
    } else if (const auto* expand = leaf.as<logical::Expand>()) {
        *out = pattern_query(ctx, _field, expand->kind, expand->pattern);
    } else if (leaf.as<logical::Exists>() != nullptr) {
        *out = std::make_shared<query_v2::AllQuery>(_field, /*nullable=*/true);
    } else if (leaf.as<logical::Empty>() != nullptr) {
        *out = std::make_shared<query_v2::BitSetQuery>(roaring::Roaring());
    } else {
        return Status::InternalError("leaf kind {} cannot be compiled on a CLucene field",
                                     leaf.value.index());
    }
    return Status::OK();
}

} // namespace doris
