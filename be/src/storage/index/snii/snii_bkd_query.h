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

#include "common/status.h"
#include "storage/index/inverted/inverted_index_query_type.h"
#include "storage/index/snii/common/slice.h"

// Predicate -> interval translation for the SNII-native BKD (design 7.1 / 10).
//
// This is the whole of what the old implementation spread across five template
// specializations of InvertedIndexVisitor<QT>, each with its own matches() and
// compare(). One interval primitive needs one translation, and it is a pure
// function of the query type -- no index, no I/O, no KeyCoder.
//
// ENCODING IS NOT DONE HERE ON PURPOSE. The caller passes bytes already produced
// by the KeyCoder of the INDEX's own field_type (INV-1); resolving that type is
// the reader's job because only the reader has read the header. Keeping the two
// apart is what makes this testable without an index at all.
namespace doris::segment_v2 {

// A closed-or-open interval in sortable-byte space, in the shape
// BkdReader::range takes. An EMPTY bound is an unbounded side.
struct BkdQueryBounds {
    snii::Slice lower;
    bool lower_inclusive = true;
    snii::Slice upper;
    bool upper_inclusive = true;
};

// Maps one supported predicate onto its interval.
//
// The open side is left UNBOUNDED rather than pinned to the type's minimum or
// maximum. The old implementation had to pin it -- its bounds were always closed
// and strictness lived in matches() -- which meant every one-sided query carried
// a type-limit encode it never used. Here the strictness is the interval's own,
// so `<` and `<=` differ by a flag and nothing else.
//
// `value` must be exactly the index's bytes_per_dim; that is the caller's
// invariant and is checked by BkdReader::range itself.
//
// Anything outside {EQUAL, LESS_THAN, LESS_EQUAL, GREATER_THAN, GREATER_EQUAL}
// comes back as INVERTED_INDEX_NOT_SUPPORTED so the caller falls back to a
// normal predicate rather than silently answering the wrong question. In
// particular RANGE_QUERY and LIST_QUERY exist in the enum but are produced only
// by the SEARCH DSL and never reach a BKD reader.
Status build_bkd_query_bounds(InvertedIndexQueryType query_type, snii::Slice value,
                              BkdQueryBounds* out);

} // namespace doris::segment_v2
