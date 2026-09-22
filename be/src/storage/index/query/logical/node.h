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

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "storage/index/inverted/query/query_info.h"

// The logical query IR shared by every index format. A query is lowered to this
// tree once: values are analyzed, operators and thresholds are resolved, and
// every leaf is bound to one physical index. Nothing here depends on how that
// index stores postings.
namespace doris::index_query::logical {

// One analyzed token: a term (or several alternative terms) at a position.
using Token = segment_v2::TermInfo;

// The physical index a leaf was bound to. `binding` is the resolver's opaque
// key; it is empty for an unbound leaf.
struct FieldRef {
    std::string name;
    std::string binding;
};

// One dictionary term taken verbatim from a clause value that was not analyzed.
struct Term {
    FieldRef field;
    std::string term;
};

// The analyzed tokens of a clause value, one or more, combined as any-of
// (`require_all` false), all-of, or at least `min_should_match` of them.
// `min_should_match` is 0 when there is no threshold; a single term never has one.
struct TermSet {
    FieldRef field;
    std::vector<std::string> terms;
    bool require_all = false;
    uint32_t min_should_match = 0;
};

// A phrase over analyzed tokens grouped by position. A slot with several terms
// accepts any of them at that position.
struct Phrase {
    FieldRef field;
    std::vector<Token> slots;
};

// PREFIX on an analyzed index. `tokens` is the analyzed value without the DSL's
// trailing '*'; `pattern` is the normalized value with that '*' kept, for
// executors that treat the whole value as one wildcard.
struct Prefix {
    FieldRef field;
    std::vector<Token> tokens;
    std::string pattern;
};

enum class ExpandKind : uint8_t { kWildcard, kRegexp };

// Every dictionary term matching `pattern`.
struct Expand {
    FieldRef field;
    ExpandKind kind = ExpandKind::kWildcard;
    std::string pattern;
};

enum class CompareOp : uint8_t { kEqual };

// A scalar comparison on a non-text index. The value stays a string; the field
// index parses it with the column type it owns.
struct Compare {
    FieldRef field;
    CompareOp op = CompareOp::kEqual;
    std::string value;
};

// Every document whose field is not NULL.
struct Exists {
    FieldRef field;
};

// No document matches; documents whose field is NULL stay UNKNOWN.
struct Empty {
    FieldRef field;
};

// UNKNOWN for every document: the field has no usable index in this segment
// (`field.binding` is empty) or its index cannot answer the clause.
struct Unknown {
    FieldRef field;
};

// Every document.
struct All {};

enum class Occur : uint8_t { kMust, kShould, kMustNot };
enum class BoolOp : uint8_t { kAnd, kOr, kNot, kOccur };

struct Node;
using NodePtr = std::shared_ptr<const Node>;

// AND / OR / NOT ignore the per-clause occur (it is kMust); kOccur uses it and
// `min_should_match` counts the kShould clauses that have to match.
struct Bool {
    BoolOp op = BoolOp::kAnd;
    std::vector<std::pair<Occur, NodePtr>> clauses;
    uint32_t min_should_match = 0;
};

struct Node {
    std::variant<Term, TermSet, Phrase, Prefix, Expand, Compare, Exists, Empty, Unknown, All, Bool>
            value;

    template <typename T>
    const T* as() const {
        return std::get_if<T>(&value);
    }

    // The field of a leaf; nullptr for Bool and All.
    const FieldRef* field() const {
        return std::visit(
                [](const auto& leaf) -> const FieldRef* {
                    if constexpr (requires { leaf.field; }) {
                        return &leaf.field;
                    } else {
                        return nullptr;
                    }
                },
                value);
    }
};

template <typename T>
NodePtr make_node(T value) {
    return std::make_shared<const Node>(Node {.value = std::move(value)});
}

} // namespace doris::index_query::logical
