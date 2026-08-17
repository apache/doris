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

// The query-side decision for the token-exists Bloom Filter absent-term fast path. Kept in its
// own narrow header (not the BF header, which is included by the writer) so it pulls in query
// types only where it is used: the reader fast path and its unit tests. Both call the same
// function, so the per-query-type / position-grouping logic is covered by tests rather than
// reimplemented in them.

#pragma once

#include "storage/index/inverted/inverted_index_query_type.h"
#include "storage/index/inverted/inverted_index_term_bloom_filter.h"
#include "storage/index/inverted/query/query_info.h"

namespace doris::segment_v2 {

// Whether the BF proves this query's result is necessarily empty (so the reader can short-circuit
// before opening the searcher). Per-query-type semantics:
//   - MATCH_ALL / EQUAL: AND -- any single ABSENT token proves empty.
//   - MATCH_PHRASE: A1 -- alternatives sharing one position are an OR slot (synonyms / CJK
//     overlap); a slot is dead only when *every* alternative at that position is ABSENT, and the
//     phrase is empty when *any* slot is dead.
//   - MATCH_ANY: OR -- empty only when *every* token is ABSENT.
//   - anything else: never proven empty (returns false).
// A2: a multi-term TermInfo is an OR of its sub-terms (ABSENT only when *all* are ABSENT);
// get_single_term() is never called on a multi-term slot.
bool bf_query_proven_empty(InvertedIndexQueryType query_type,
                           const InvertedIndexQueryInfo& query_info,
                           const InvertedIndexTermBloomFilter& bf);

} // namespace doris::segment_v2
