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
#include "storage/index/query/docid_sink.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/reader/logical_index_reader.h"

namespace doris::snii::query::internal {

struct ResolvedDocidPosting {
    // The caller owns entry until the synchronous read completes.
    const format::DictEntry& entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
};

// Decodes the docid-only posting for a resolved term. The caller owns term
// lookup and can batch/plan lookups independently; this module owns only the
// three posting encodings (inline, slim pod_ref, windowed pod_ref).
Status read_docid_posting(const reader::LogicalIndexReader& idx, const format::DictEntry& entry,
                          uint64_t frq_base, uint64_t prx_base, std::vector<uint32_t>* docids);

Status read_docid_posting(const reader::LogicalIndexReader& idx, const format::DictEntry& entry,
                          uint64_t frq_base, uint64_t prx_base, index_query::DocIdSink* sink);

// Reads postings in one I/O round and emits them into a deduplicating sink.
// Dense windows use ranges; other postings share one document buffer.
Status emit_docid_postings_streamed(const reader::LogicalIndexReader& idx,
                                    const std::vector<ResolvedDocidPosting>& postings,
                                    index_query::DocIdSink* sink);

} // namespace doris::snii::query::internal
