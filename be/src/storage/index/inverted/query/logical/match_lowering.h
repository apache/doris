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
#include <functional>
#include <string_view>
#include <utility>
#include <vector>

#include "common/status.h"
#include "storage/index/inverted/common/inverted_index_query_type.h"
#include "storage/index/inverted/query/logical/node.h"

namespace doris::index_query::logical {

// Tokenizes a value with the analyzer of the index a predicate runs on.
using AnalyzeValue = std::function<Status(std::string_view value, std::vector<Token>* out)>;

// Removes a trailing " ~N" or " ~N+" from a phrase and returns its slop and ordering rule.
// Without such a suffix the value is unchanged and the slop is 0.
std::pair<int32_t, bool> take_slop(std::string_view* value);

// Lowers a MATCH or EQUAL predicate; no analyzer means the value is one keyword term.
// REGEXP/WILDCARD patterns stay raw, while phrase tokens follow analysis order and a trailing
// " ~N" or " ~N+" sets unordered or ordered slop.
Status lower_match(segment_v2::InvertedIndexQueryType query_type, std::string_view value,
                   const AnalyzeValue& analyze, Node* out);

// The MATCH query type a lowered leaf answers as: a term set is MATCH_ANY or MATCH_ALL, a phrase
// MATCH_PHRASE, MATCH_PHRASE_PREFIX or MATCH_PHRASE_EDGE, and an expansion the type of its kind.
// A leaf without terms answers as UNKNOWN_QUERY.
segment_v2::InvertedIndexQueryType leaf_query_type(const Node& leaf);

} // namespace doris::index_query::logical
