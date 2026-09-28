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

#include <algorithm>
#include <cctype>
#include <charconv>
#include <iterator>
#include <string_view>
#include <unordered_map>
#include <utility>

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

// Moves the trailing " ~N" or " ~N+" of a MATCH_PHRASE value into `phrase`.
void take_slop(std::string_view* value, Phrase* phrase) {
    const size_t space = value->find_last_of(' ');
    if (space == std::string_view::npos || value->substr(space + 1, 1) != "~") {
        return;
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
        return;
    }
    phrase->slop = slop;
    phrase->ordered = ordered;
    *value = value->substr(0, space);
}

NodePtr lower_direct_index_leaf(const std::string& clause_type, FieldRef field,
                                const std::string& value) {
    if (clause_type == "TERM" || clause_type == "EXACT") {
        return make_node(
                Compare {.field = std::move(field), .op = CompareOp::kEqual, .value = value});
    }
    return make_node(Unknown {.field = std::move(field)});
}

// WILDCARD, REGEXP and PREFIX match dictionary terms against a pattern. As in Elasticsearch's
// query_string, a prefix or a glob is normalized the way the index normalizes its terms and is
// never analyzed, and a regular expression is taken as written.
Status lower_pattern_leaf(const std::string& clause_type, const FieldProps& props,
                          FieldCatalog& catalog, FieldRef field, const std::string& value,
                          NodePtr* out) {
    if (clause_type == "REGEXP") {
        // SEARCH matches a regular expression against whole terms on every format.
        *out = make_node(Expand {.field = std::move(field),
                                 .kind = ExpandKind::kRegexp,
                                 .pattern = "^(" + value + ")$"});
        return Status::OK();
    }
    if (clause_type == "WILDCARD" && value == "*") {
        *out = make_node(Exists {.field = std::move(field)});
        return Status::OK();
    }
    const bool prefix = clause_type == "PREFIX";
    // The DSL keeps a PREFIX value's trailing '*'.
    std::string pattern =
            prefix && value.ends_with('*') ? value.substr(0, value.size() - 1) : value;
    if (props.analyzed) {
        std::string normalized;
        RETURN_IF_ERROR(catalog.normalize(props, pattern, &normalized));
        pattern = std::move(normalized);
    }
    *out = make_node(Expand {.field = std::move(field),
                             .kind = prefix ? ExpandKind::kPrefix : ExpandKind::kWildcard,
                             .pattern = std::move(pattern)});
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
        Node node;
        lower_phrase(Phrase {.field = std::move(field), .slots = {}}, std::move(tokens), &node);
        *out = std::make_shared<const Node>(std::move(node));
        return Status::OK();
    }
    std::vector<std::string> terms = flatten(std::move(tokens));
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
    FieldRef field {.name = clause.field_name, .binding = props.binding};
    if (!props.bound) {
        *out = make_node(Unknown {.field = std::move(field)});
        return Status::OK();
    }
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
    Phrase phrase;
    if (query_type == InvertedIndexQueryType::MATCH_PHRASE_QUERY) {
        take_slop(&value, &phrase);
    }
    std::vector<Token> tokens;
    RETURN_IF_ERROR(analyze(value, &tokens));
    // MATCH places a phrase's tokens by their order, so tokens an analyzer stacks at one position
    // run one after another.
    for (size_t i = 0; i < tokens.size(); ++i) {
        tokens[i].position = static_cast<int32_t>(i + 1);
    }
    if (tokens.empty()) {
        out->value = Empty {};
    } else if (query_type == InvertedIndexQueryType::MATCH_PHRASE_QUERY) {
        lower_phrase(std::move(phrase), std::move(tokens), out);
    } else if (query_type == InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY) {
        lower_phrase_prefix(std::move(tokens), out);
    } else if (query_type == InvertedIndexQueryType::MATCH_PHRASE_EDGE_QUERY) {
        lower_phrase_edge(std::move(tokens), out);
    } else {
        // A row equal to the value holds every one of its tokens.
        out->value =
                TermSet {.field = {},
                         .terms = flatten(std::move(tokens)),
                         .require_all = query_type == InvertedIndexQueryType::MATCH_ALL_QUERY ||
                                        query_type == InvertedIndexQueryType::EQUAL_QUERY};
    }
    return Status::OK();
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

namespace doris::index_query::logical {

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
