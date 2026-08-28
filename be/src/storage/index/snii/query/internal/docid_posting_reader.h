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
#include <vector>

#include "common/status.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/query/docid_sink.h"
#include "storage/index/snii/reader/logical_index_reader.h"

namespace doris::snii::query::internal {

struct ResolvedDocidPosting {
    format::DictEntry entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
};

// Decodes the docid-only posting for a resolved term. The caller owns term
// lookup and can batch/plan lookups independently; this module owns only the
// three posting encodings (inline, slim pod_ref, windowed pod_ref).
Status read_docid_posting(const reader::LogicalIndexReader& idx, const format::DictEntry& entry,
                          uint64_t frq_base, uint64_t prx_base, std::vector<uint32_t>* docids);

Status read_docid_posting(const reader::LogicalIndexReader& idx, const format::DictEntry& entry,
                          uint64_t frq_base, uint64_t prx_base, query::DocIdSink* sink);

// Batch counterpart for multi-term docid-only operators. Windowed terms share one
// prelude fetch round and one docid fetch round, so OR-style operators pay by
// stage rather than by term.
Status read_docid_postings_batched(const reader::LogicalIndexReader& idx,
                                   const std::vector<ResolvedDocidPosting>& postings,
                                   std::vector<std::vector<uint32_t>>* docids);

// Streaming counterpart of read_docid_postings_batched for a dedup-capable sink
// (DocIdSink::dedups()==true, e.g. a Roaring bitmap). Shares the exact same single
// docid fetch round, but decodes each posting straight into the sink -- dense-full
// windows via append_range (run-preserving), the rest via append_sorted from one
// reused scratch buffer -- so no per-term vector or K-way merge accumulator is
// materialized. The sink dedups/orders across postings. One I/O round is preserved.
Status emit_docid_postings_streamed(const reader::LogicalIndexReader& idx,
                                    const std::vector<ResolvedDocidPosting>& postings,
                                    query::DocIdSink* sink);

} // namespace doris::snii::query::internal
