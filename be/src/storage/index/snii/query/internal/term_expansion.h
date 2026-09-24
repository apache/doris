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

#include "common/status.h"
#include "storage/index/query/docid_sink.h"
#include "storage/index/query/term_pattern.h"
#include "storage/index/snii/reader/logical_index_reader.h"

namespace doris::snii::query::internal {

// Enumerates the logical plain terms `pattern` matches, at most `max_expansions`
// of them when it is positive, while retaining each matching physical DictEntry
// for direct posting resolution. PrefixHit::term is decoded logical text;
// PrefixHit::entry.term remains the physical dictionary key.
Status visit_expanded_plain_terms(const reader::LogicalIndexReader& idx,
                                  index_query::TermPattern& pattern,
                                  const reader::LogicalIndexReader::PrefixHitVisitor& visitor,
                                  int32_t max_expansions = 0);

// Emits the sorted docid union of the terms `pattern` matches. PrefixHit carries
// the DictEntry and block bases, so callers avoid a second lookup per expanded term.
Status emit_expanded_docid_union(const reader::LogicalIndexReader& idx,
                                 index_query::TermPattern& pattern,
                                 index_query::DocIdSink* const sink, int32_t max_expansions = 0);

} // namespace doris::snii::query::internal
