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

#include "storage/index/inverted/query/logical/match_lowering.h"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <string_view>
#include <utility>

#include "storage/index/inverted/query/logical/lowering_common.h"

namespace doris::index_query::logical {

using segment_v2::InvertedIndexQueryType;
using detail::flatten;
using detail::lower_phrase;
using detail::lower_phrase_prefix;
using detail::lower_phrase_edge;

std::pair<int32_t, bool> take_slop(std::string_view* value) {
    const size_t space = value->find_last_of(' ');
    if (space == std::string_view::npos || value->substr(space + 1, 1) != "~") {
        return {};
    }
    std::string_view digits = value->substr(space + 2);
    const bool ordered = digits.size() > 1 && digits.back() == '+';
    if (ordered) {
        digits.remove_suffix(1);
    }
    int32_t slop = 0;
    if (digits.empty() ||
        !std::ranges::all_of(digits, [](unsigned char c) { return std::isdigit(c) != 0; }) ||
        std::from_chars(digits.data(), digits.data() + digits.size(), slop).ec != std::errc()) {
        return {};
    }
    *value = value->substr(0, space);
    return {slop, ordered};
}

Status lower_match(InvertedIndexQueryType query_type, std::string_view value,
                   const AnalyzeValue& analyze, Node* out) {
    DCHECK(out != nullptr);
    switch (query_type) {
    case InvertedIndexQueryType::MATCH_REGEXP_QUERY:
    case InvertedIndexQueryType::WILDCARD_QUERY:
        out->value = Expand {.field = {},
                             .kind = query_type == InvertedIndexQueryType::MATCH_REGEXP_QUERY
                                             ? ExpandKind::kRegexp
                                             : ExpandKind::kWildcard,
                             .pattern = std::string(value)};
        return Status::OK();
    case InvertedIndexQueryType::EQUAL_QUERY:
    case InvertedIndexQueryType::MATCH_ANY_QUERY:
    case InvertedIndexQueryType::MATCH_ALL_QUERY:
    case InvertedIndexQueryType::MATCH_PHRASE_QUERY:
    case InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY:
    case InvertedIndexQueryType::MATCH_PHRASE_EDGE_QUERY:
        break;
    default:
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "no index query lowers query type {}", query_type_to_string(query_type));
    }
    const auto [slop, ordered] = query_type == InvertedIndexQueryType::MATCH_PHRASE_QUERY
                                         ? take_slop(&value)
                                         : std::pair<int32_t, bool> {};
    if (!analyze) {
        if (query_type == InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY ||
            query_type == InvertedIndexQueryType::MATCH_PHRASE_EDGE_QUERY) {
            out->value =
                    Expand {.field = {},
                            .kind = query_type == InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY
                                            ? ExpandKind::kPrefix
                                            : ExpandKind::kContains,
                            .pattern = std::string(value)};
        } else {
            out->value = Term {.field = {}, .term = std::string(value)};
        }
        return Status::OK();
    }
    std::vector<Token> tokens;
    RETURN_IF_ERROR(analyze(value, &tokens));
    if (is_phrase_query(query_type)) {
        // MATCH runs a phrase's tokens in order, including tokens stacked at one position.
        for (size_t i = 0; i < tokens.size(); ++i) {
            tokens[i].position = static_cast<int32_t>(i + 1);
        }
    }
    if (tokens.empty()) {
        out->value = Empty {};
    } else if (query_type == InvertedIndexQueryType::MATCH_PHRASE_QUERY) {
        lower_phrase(Phrase {.field = {}, .slots = {}, .slop = slop, .ordered = ordered},
                     std::move(tokens), out);
    } else if (query_type == InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY) {
        lower_phrase_prefix(std::move(tokens), out);
    } else if (query_type == InvertedIndexQueryType::MATCH_PHRASE_EDGE_QUERY) {
        lower_phrase_edge(std::move(tokens), out);
    } else if (tokens.size() == 1 && tokens.front().is_single_term()) {
        out->value.emplace<Term>().term = std::move(std::get<std::string>(tokens.front().term));
    } else {
        // A row equal to the value holds every one of its tokens.
        auto& terms = out->value.emplace<TermSet>();
        terms.terms = flatten(std::move(tokens));
        terms.require_all = query_type == InvertedIndexQueryType::MATCH_ALL_QUERY ||
                            query_type == InvertedIndexQueryType::EQUAL_QUERY;
    }
    return Status::OK();
}

InvertedIndexQueryType leaf_query_type(const Node& leaf) {
    if (leaf.as<Term>() != nullptr) {
        return InvertedIndexQueryType::EQUAL_QUERY;
    }
    if (const auto* set = leaf.as<TermSet>()) {
        return set->require_all ? InvertedIndexQueryType::MATCH_ALL_QUERY
                                : InvertedIndexQueryType::MATCH_ANY_QUERY;
    }
    if (const auto* phrase = leaf.as<Phrase>()) {
        if (phrase->suffix) {
            return InvertedIndexQueryType::MATCH_PHRASE_EDGE_QUERY;
        }
        return phrase->prefix ? InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY
                              : InvertedIndexQueryType::MATCH_PHRASE_QUERY;
    }
    if (const auto* expand = leaf.as<Expand>()) {
        switch (expand->kind) {
        case ExpandKind::kPrefix:
            // A one-term phrase prefix runs as a prefix of that term.
            return InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY;
        case ExpandKind::kRegexp:
            return InvertedIndexQueryType::MATCH_REGEXP_QUERY;
        case ExpandKind::kContains:
            // A one-term edge phrase runs as the terms that contain it.
            return InvertedIndexQueryType::MATCH_PHRASE_EDGE_QUERY;
        case ExpandKind::kWildcard:
        default:
            return InvertedIndexQueryType::WILDCARD_QUERY;
        }
    }
    return InvertedIndexQueryType::UNKNOWN_QUERY;
}

} // namespace doris::index_query::logical
