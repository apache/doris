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

#include "storage/index/query/logical/search_lowering.h"

#include <unordered_map>
#include <utility>

#include "util/string_util.h"

namespace doris::index_query::logical {

using segment_v2::InvertedIndexQueryType;

namespace {

bool is_compound(const std::string& clause_type) {
    return clause_type == "AND" || clause_type == "OR" || clause_type == "NOT" ||
           clause_type == "OCCUR_BOOLEAN";
}

bool is_pattern_clause(const std::string& clause_type) {
    return clause_type == "WILDCARD" || clause_type == "REGEXP" || clause_type == "PREFIX";
}

// Clause types whose value is analyzed on a tokenized index.
bool is_analyzed_clause(const std::string& clause_type) {
    return clause_type == "TERM" || clause_type == "PHRASE" || clause_type == "MATCH" ||
           clause_type == "ANY" || clause_type == "ALL";
}

Occur to_occur(TSearchOccur::type occur) {
    switch (occur) {
    case TSearchOccur::SHOULD:
        return Occur::kShould;
    case TSearchOccur::MUST_NOT:
        return Occur::kMustNot;
    case TSearchOccur::MUST:
    default:
        return Occur::kMust;
    }
}

std::string normalize_pattern(const FieldProps& props, const std::string& value) {
    return props.lowercase_patterns ? to_lower(value) : value;
}

void append_terms(const Token& token, std::vector<std::string>* terms) {
    if (token.is_single_term()) {
        terms->push_back(token.get_single_term());
    } else {
        const auto& many = token.get_multi_terms();
        terms->insert(terms->end(), many.begin(), many.end());
    }
}

// Groups tokens that share a position into one slot.
std::vector<Token> group_by_position(const std::vector<Token>& tokens) {
    std::vector<Token> slots;
    size_t i = 0;
    while (i < tokens.size()) {
        const int32_t position = tokens[i].position;
        std::vector<std::string> alternatives;
        while (i < tokens.size() && tokens[i].position == position) {
            append_terms(tokens[i], &alternatives);
            ++i;
        }
        Token slot;
        slot.position = position;
        if (alternatives.size() == 1) {
            slot.term = std::move(alternatives.front());
        } else {
            slot.term = std::move(alternatives);
        }
        slots.push_back(std::move(slot));
    }
    return slots;
}

std::vector<std::string> flatten(const std::vector<Token>& tokens) {
    std::vector<std::string> terms;
    terms.reserve(tokens.size());
    for (const auto& token : tokens) {
        append_terms(token, &terms);
    }
    return terms;
}

NodePtr lower_direct_index_leaf(const std::string& clause_type, FieldRef field,
                                const std::string& value) {
    if (clause_type == "TERM" || clause_type == "EXACT") {
        return make_node(
                Compare {.field = std::move(field), .op = CompareOp::kEqual, .value = value});
    }
    return make_node(Unknown {});
}

// WILDCARD, REGEXP and PREFIX match dictionary terms against a pattern.
Status lower_pattern_leaf(const std::string& clause_type, const FieldProps& props,
                          FieldCatalog& catalog, FieldRef field, const std::string& value,
                          NodePtr* out) {
    if (clause_type == "REGEXP") {
        *out = make_node(
                Expand {.field = std::move(field), .kind = ExpandKind::kRegexp, .pattern = value});
        return Status::OK();
    }
    if (clause_type == "WILDCARD" && value == "*") {
        *out = make_node(Exists {.field = std::move(field)});
        return Status::OK();
    }
    if (clause_type == "WILDCARD" || !props.analyzed) {
        *out = make_node(Expand {.field = std::move(field),
                                 .kind = ExpandKind::kWildcard,
                                 .pattern = normalize_pattern(props, value)});
        return Status::OK();
    }
    // PREFIX on an analyzed index: the DSL keeps the trailing '*' in the value, so
    // analyze the stem only.
    std::string stem = value;
    if (!stem.empty() && stem.back() == '*') {
        stem.pop_back();
    }
    std::vector<Token> tokens;
    RETURN_IF_ERROR(catalog.analyze(props, stem, &tokens));
    if (tokens.empty()) {
        *out = make_node(Empty {.field = std::move(field)});
        return Status::OK();
    }
    *out = make_node(Prefix {.field = std::move(field),
                             .tokens = std::move(tokens),
                             .pattern = normalize_pattern(props, value)});
    return Status::OK();
}

// TERM, PHRASE, MATCH, ANY and ALL on an analyzed index.
Status lower_analyzed_leaf(const std::string& clause_type, const LoweringOptions& options,
                           const FieldProps& props, FieldCatalog& catalog, FieldRef field,
                           const std::string& value, NodePtr* out) {
    std::vector<Token> tokens;
    RETURN_IF_ERROR(catalog.analyze(props, value, &tokens));
    if (tokens.empty()) {
        *out = make_node(Empty {.field = std::move(field)});
        return Status::OK();
    }
    if (clause_type == "PHRASE") {
        std::vector<Token> slots = group_by_position(tokens);
        if (slots.size() > 1) {
            *out = make_node(Phrase {.field = std::move(field), .slots = std::move(slots)});
            return Status::OK();
        }
        // One position: any of its terms.
        tokens = std::move(slots);
    }
    std::vector<std::string> terms = flatten(tokens);
    bool require_all = false;
    uint32_t min_should_match = 0;
    if (clause_type == "TERM") {
        require_all = options.default_operator == "and";
        // The threshold applies to the boolean the terms form; one term forms none.
        if (options.minimum_should_match > 0 && terms.size() > 1) {
            min_should_match = static_cast<uint32_t>(options.minimum_should_match);
        }
    } else if (clause_type == "ALL") {
        require_all = true;
    }
    *out = make_node(TermSet {.field = std::move(field),
                              .terms = std::move(terms),
                              .require_all = require_all,
                              .min_should_match = min_should_match});
    return Status::OK();
}

Status lower_leaf(const TSearchClause& clause, const LoweringOptions& options,
                  FieldCatalog& catalog, NodePtr* out) {
    if (!clause.__isset.field_name || !clause.__isset.value) {
        return Status::InvalidArgument("search clause missing field_name or value");
    }
    const std::string& clause_type = clause.clause_type;
    const std::string& value = clause.value;

    FieldProps props;
    RETURN_IF_ERROR(
            catalog.resolve(clause.field_name, search_clause_query_type(clause_type), &props));
    if (!props.bound) {
        *out = make_node(Unknown {});
        return Status::OK();
    }
    FieldRef field {.name = clause.field_name, .binding = props.binding};
    if (props.direct_index) {
        *out = lower_direct_index_leaf(clause_type, std::move(field), value);
        return Status::OK();
    }
    if (is_pattern_clause(clause_type)) {
        return lower_pattern_leaf(clause_type, props, catalog, std::move(field), value, out);
    }
    if (is_analyzed_clause(clause_type) && props.analyzed) {
        return lower_analyzed_leaf(clause_type, options, props, catalog, std::move(field), value,
                                   out);
    }
    // EXACT, the unimplemented RANGE and LIST, unknown clause types, and analyzed
    // clause types on an untokenized index all keep the raw value as one term.
    *out = make_node(Term {.field = std::move(field), .term = value});
    return Status::OK();
}

Status lower_compound(const TSearchClause& clause, const LoweringOptions& options,
                      FieldCatalog& catalog, NodePtr* out) {
    Bool node;
    const std::string& clause_type = clause.clause_type;
    if (clause_type == "AND") {
        node.op = BoolOp::kAnd;
    } else if (clause_type == "OR") {
        node.op = BoolOp::kOr;
    } else if (clause_type == "NOT") {
        node.op = BoolOp::kNot;
    } else {
        node.op = BoolOp::kOccur;
        if (clause.__isset.minimum_should_match && clause.minimum_should_match > 0) {
            node.min_should_match = static_cast<uint32_t>(clause.minimum_should_match);
        }
    }
    if (clause.__isset.children) {
        node.clauses.reserve(clause.children.size());
        for (const auto& child : clause.children) {
            NodePtr lowered;
            RETURN_IF_ERROR(lower_search_clause(child, options, catalog, &lowered));
            const Occur occur = node.op == BoolOp::kOccur && child.__isset.occur
                                        ? to_occur(child.occur)
                                        : Occur::kMust;
            node.clauses.emplace_back(occur, std::move(lowered));
        }
    }
    *out = make_node(std::move(node));
    return Status::OK();
}

} // namespace

InvertedIndexQueryType search_clause_query_type(const std::string& clause_type) {
    static const std::unordered_map<std::string, InvertedIndexQueryType> query_types = {
            {"AND", InvertedIndexQueryType::BOOLEAN_QUERY},
            {"OR", InvertedIndexQueryType::BOOLEAN_QUERY},
            {"NOT", InvertedIndexQueryType::BOOLEAN_QUERY},
            {"OCCUR_BOOLEAN", InvertedIndexQueryType::BOOLEAN_QUERY},
            {"NESTED", InvertedIndexQueryType::BOOLEAN_QUERY},
            // These operate on single index terms, so a tokenized index is preferred when a
            // column has both a tokenized and an untokenized one.
            {"TERM", InvertedIndexQueryType::MATCH_ANY_QUERY},
            {"PREFIX", InvertedIndexQueryType::MATCH_ANY_QUERY},
            {"WILDCARD", InvertedIndexQueryType::MATCH_ANY_QUERY},
            {"REGEXP", InvertedIndexQueryType::MATCH_ANY_QUERY},
            {"RANGE", InvertedIndexQueryType::RANGE_QUERY},
            {"LIST", InvertedIndexQueryType::LIST_QUERY},
            {"PHRASE", InvertedIndexQueryType::MATCH_PHRASE_QUERY},
            {"MATCH", InvertedIndexQueryType::MATCH_ANY_QUERY},
            {"ANY", InvertedIndexQueryType::MATCH_ANY_QUERY},
            {"ALL", InvertedIndexQueryType::MATCH_ALL_QUERY},
            // EXACT prefers the untokenized index.
            {"EXACT", InvertedIndexQueryType::EQUAL_QUERY},
    };
    auto it = query_types.find(clause_type);
    return it == query_types.end() ? InvertedIndexQueryType::EQUAL_QUERY : it->second;
}

Status lower_search_clause(const TSearchClause& clause, const LoweringOptions& options,
                           FieldCatalog& catalog, NodePtr* out) {
    DCHECK(out != nullptr);
    *out = nullptr;
    const std::string& clause_type = clause.clause_type;
    if (clause_type == "MATCH_ALL_DOCS") {
        *out = make_node(All {});
        return Status::OK();
    }
    if (clause_type == "NESTED") {
        return Status::InvalidArgument("NESTED clause must be evaluated at top level");
    }
    if (is_compound(clause_type)) {
        return lower_compound(clause, options, catalog, out);
    }
    return lower_leaf(clause, options, catalog, out);
}

} // namespace doris::index_query::logical
