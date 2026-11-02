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

#include "storage/index/inverted/query/logical/lowering_common.h"

#include <iterator>
#include <utility>

namespace doris::index_query::logical::detail {

void append_terms(Token token, std::vector<std::string>* terms) {
    if (token.is_single_term()) {
        terms->push_back(std::move(std::get<std::string>(token.term)));
    } else {
        auto& many = std::get<std::vector<std::string>>(token.term);
        terms->insert(terms->end(), std::make_move_iterator(many.begin()),
                      std::make_move_iterator(many.end()));
    }
}

// Groups tokens that share a position into one slot, in place.
std::vector<Token> group_by_position(std::vector<Token> tokens) {
    size_t slots = 0;
    for (size_t i = 0; i < tokens.size(); ++i) {
        if (slots > 0 && tokens[slots - 1].position == tokens[i].position) {
            Token& slot = tokens[slots - 1];
            if (slot.is_single_term()) {
                slot.term = std::vector<std::string> {std::move(std::get<std::string>(slot.term))};
            }
            append_terms(std::move(tokens[i]), &std::get<std::vector<std::string>>(slot.term));
            continue;
        }
        if (slots != i) {
            tokens[slots] = std::move(tokens[i]);
        }
        ++slots;
    }
    tokens.resize(slots);
    return tokens;
}

std::vector<std::string> flatten(std::vector<Token> tokens) {
    std::vector<std::string> terms;
    terms.reserve(tokens.size());
    for (auto& token : tokens) {
        append_terms(std::move(token), &terms);
    }
    return terms;
}

// Sets `out` to `phrase` over `tokens`, or to any of their terms when they take one position.
void lower_phrase(Phrase phrase, std::vector<Token> tokens, Node* out) {
    phrase.slots = group_by_position(std::move(tokens));
    if (phrase.slots.size() > 1) {
        out->value = std::move(phrase);
        return;
    }
    out->value =
            TermSet {.field = std::move(phrase.field), .terms = flatten(std::move(phrase.slots))};
}

// Sets `out` to a phrase whose last slot is a prefix; a single token is just that prefix.
void lower_phrase_prefix(std::vector<Token> tokens, Node* out) {
    std::vector<Token> slots = group_by_position(std::move(tokens));
    if (slots.size() == 1 && slots.front().is_single_term()) {
        out->value = Expand {.field = {},
                             .kind = ExpandKind::kPrefix,
                             .pattern = std::move(std::get<std::string>(slots.front().term))};
        return;
    }
    out->value = Phrase {.field = {}, .slots = std::move(slots), .prefix = true};
}

// Sets `out` to a phrase whose first slot takes the terms that end with its token and whose last
// slot the terms that start with its token; a single token takes every term that contains it.
void lower_phrase_edge(std::vector<Token> tokens, Node* out) {
    std::vector<Token> slots = group_by_position(std::move(tokens));
    if (slots.size() == 1 && slots.front().is_single_term()) {
        out->value = Expand {.field = {},
                             .kind = ExpandKind::kContains,
                             .pattern = std::move(std::get<std::string>(slots.front().term))};
        return;
    }
    out->value = Phrase {.field = {}, .slots = std::move(slots), .prefix = true, .suffix = true};
}

} // namespace doris::index_query::logical::detail
