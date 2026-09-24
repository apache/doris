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

#include <gen_cpp/Exprs_types.h>

#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "storage/index/inverted/inverted_index_query_type.h"
#include "storage/index/query/logical/node.h"

// Lowers the SEARCH DSL clause tree and MATCH predicates to the logical IR. This
// is the only place that interprets clause types and phrase slop, analyzes
// values, and applies `default_operator`, `minimum_should_match` and pattern
// normalization; every index format then executes the same tree.
namespace doris::index_query::logical {

// What lowering needs to know about the index a field resolves to.
struct FieldProps {
    // False when the field has no usable index in this segment.
    bool bound = false;
    // A scalar index: only TERM and EXACT apply, as a comparison.
    bool direct_index = false;
    // The index tokenizes values, so clause values are analyzed.
    bool analyzed = false;
    std::string binding;
};

class FieldCatalog {
public:
    virtual ~FieldCatalog() = default;

    // `query_type` is the reader-selection hint the clause implies (see
    // search_clause_query_type). An unbound field returns OK with bound=false.
    virtual Status resolve(const std::string& field, segment_v2::InvertedIndexQueryType query_type,
                           FieldProps* out) = 0;

    // Tokenizes `value` with the analyzer of the index `props` resolved to.
    virtual Status analyze(const FieldProps& props, const std::string& value,
                           std::vector<Token>* out) = 0;

    // Normalizes `value` as the analyzed index `props` resolved to normalizes its terms, without
    // splitting it.
    virtual Status normalize(const FieldProps& props, const std::string& value,
                             std::string* out) = 0;
};

struct LoweringOptions {
    std::string default_operator = "or";
    // The DSL-level threshold for multi-token TERM values; -1 when unset.
    int32_t minimum_should_match = -1;
};

// The reader-selection hint for a leaf clause type. TERM, WILDCARD, PREFIX and
// REGEXP prefer a tokenized index; EXACT prefers an untokenized one.
segment_v2::InvertedIndexQueryType search_clause_query_type(const std::string& clause_type);

// Lowers `clause` and its children. NESTED must be handled by the caller.
Status lower_search_clause(const TSearchClause& clause, const LoweringOptions& options,
                           FieldCatalog& catalog, NodePtr* out);

// Tokenizes a value with the analyzer of the index a predicate runs on.
using AnalyzeValue = std::function<Status(std::string_view value, std::vector<Token>* out)>;

// Lowers a predicate on one index: MATCH_ANY, MATCH_ALL, MATCH_PHRASE, MATCH_PHRASE_PREFIX,
// MATCH_REGEXP, EQUAL or WILDCARD. The value is analyzed, except a MATCH_REGEXP or WILDCARD
// pattern, which is taken as written, and its tokens take positions in the order the analyzer
// emits them. A MATCH_PHRASE value ending in " ~N" or " ~N+" has slop N, and "+" keeps the tokens
// in order.
Status lower_match(segment_v2::InvertedIndexQueryType query_type, std::string_view value,
                   const AnalyzeValue& analyze, Node* out);

} // namespace doris::index_query::logical
