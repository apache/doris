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
#include <string>
#include <string_view>

#include "common/status.h"
#include "storage/index/snii/reader/logical_index_reader.h"

// Forward-declare the CRoaring C++ bitmap so this header stays free of the
// (large) roaring include; the concrete type is only needed in the .cpp.
namespace roaring {
class Roaring;
} // namespace roaring

// count_query -- G02 count-only fast path primitives. They answer "how many
// docs match" from DICT ENTRIES ALONE (per-term df), never touching the .frq
// posting bytes, for the two shapes whose df IS the exact per-segment match
// count:
//
//   - single exact term: DictEntry.df is by definition the number of docs
//     containing the term in this segment (nulls are never in postings, so df
//     already matches MATCH's "null never matches" semantics);
//   - 2-term consecutive phrase answered by a hidden phrase-bigram dict HIT:
//     per the G01 contract, bigram docid membership IS the phrase answer
//     (TryTwoTermPhraseBigram in phrase_query.cpp), so the bigram entry's df
//     is the exact phrase doc count.
//
// Everything else -- multi-term OR/AND, prefix/regexp/wildcard expansion,
// pruned or absent bigrams, positionless indexes -- must fall through to the
// normal decode path (*handled stays false). Deletes/extra predicates are a
// CALLER responsibility: these primitives count docs in the raw segment and
// are only safe when the caller proved nothing else filters the row space
// (see SniiIndexReader::_try_count_only_fastpath and
// SegmentIterator guards in count_on_index_fastpath.h).
namespace doris::snii::query {

// df of `term` in this segment without decoding postings. An absent term is a
// deterministic answer too: *count = 0 (mirrors term_query's empty result).
// Increments the count_fastpath_hits test seam.
Status count_only_term_df(const reader::LogicalIndexReader& idx, std::string_view term,
                          uint64_t* count);

// Count-only answer for the 2-term consecutive phrase [left, right] via the
// hidden phrase-bigram term. *handled = true ONLY on a bigram dict HIT (a G01
// survivor on pruned segments, or any materialized pair on legacy segments);
// then *count = bigram df. Every miss falls through (*handled = false) --
// including the legacy "miss == no adjacency" case -- so the generic phrase
// path stays the single owner of miss semantics. Also falls through when the
// index has no positions (the normal phrase path errors there and the fast
// path must not mask it) or when either term is not bigram-indexable.
// Increments the count_fastpath_hits test seam only when handled.
Status count_only_two_term_phrase_bigram_df(const reader::LogicalIndexReader& idx,
                                            const std::string& left, const std::string& right,
                                            bool* handled, uint64_t* count);

// Builds the fabricated count bitmap for a segment WITH a null bitmap: exactly
// `count` row ids DISJOINT from `nulls` (the first `count` non-null row ids,
// all < count + |nulls|). Why disjoint: the MATCH machinery unconditionally
// subtracts the segment null bitmap from every index result
// (FunctionMatchBase -> InvertedIndexResultBitmap::mask_out_null). Real
// postings never contain null docs -- the writer adds NO tokens for a null doc
// (scalar add_nulls) and a NULL array row is stored as an empty range (zero
// tokens) -- so that subtraction is a no-op on true results and df already IS
// the exact match count regardless of nulls. A naive [0, df) range however MAY
// collide with null row ids and be shrunk by mask_out_null; picking the ids
// from the non-null space makes the subtraction provably a no-op, preserving
// cardinality == df end to end.
//
// The window bound is doc-count-free: count counts only non-null docs, so
// count + |nulls| <= segment doc count and [0, count + |nulls|) always holds
// >= count non-null ids; every fabricated id therefore stays inside the
// segment's [0, num_rows) row space. Errors (id space would exceed the uint32
// docid domain, or the window unexpectedly holds fewer than `count` survivors)
// only occur on a corrupt index; callers treat them as "fall through to the
// decode path", never as a fabricated answer.
Status fabricate_null_disjoint_count_bitmap(uint64_t count, const roaring::Roaring& nulls,
                                            roaring::Roaring* out);

} // namespace doris::snii::query
