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
#include <vector>

#include "common/status.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/format/frq_prelude.h"
#include "storage/index/snii/io/batch_range_fetcher.h"
#include "storage/index/snii/reader/logical_index_reader.h"

namespace doris::snii::query::internal {

struct ResolvedQueryTerm {
    format::DictEntry entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
};

struct TermPlan {
    format::DictEntry entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    uint32_t df = 0;
    size_t order = 0;
    size_t frq_handle = 0;
    size_t prx_handle = 0;
    size_t prelude_handle = 0;
    bool pod_ref = false;
    bool windowed = false;
    format::FrqPreludeReader prelude;
};

struct DocidChunk {
    std::vector<uint32_t> docids;
    std::vector<uint32_t> prx_doc_ordinals;
    uint32_t prx_doc_count = 0;
    bool windowed = false;
    uint32_t window = 0;
};

struct DocidSource {
    std::vector<DocidChunk> chunks;
    bool docids_are_final_candidates = false;
};

Status resolve_query_term(const reader::LogicalIndexReader& idx, const std::string& term,
                          ResolvedQueryTerm* resolved, bool* found);

Status plan_terms(const reader::LogicalIndexReader& idx, const std::vector<std::string>& terms,
                  io::BatchRangeFetcher* fetcher, std::vector<TermPlan>* plans, bool* all_present,
                  bool need_positions);

Status plan_resolved_terms(const reader::LogicalIndexReader& idx,
                           const std::vector<ResolvedQueryTerm>& terms,
                           io::BatchRangeFetcher* fetcher, std::vector<TermPlan>* plans,
                           bool need_positions);

Status open_preludes(const io::BatchRangeFetcher& fetcher, std::vector<TermPlan>* plans,
                     bool need_positions);

Status inline_dd_region(const format::DictEntry& entry, Slice* out);

Status build_docid_only_conjunction(const reader::LogicalIndexReader& idx,
                                    const io::BatchRangeFetcher& round1,
                                    const std::vector<TermPlan>& plans,
                                    std::vector<uint32_t>* candidates);

Status build_docid_only_conjunction(const reader::LogicalIndexReader& idx,
                                    const io::BatchRangeFetcher& round1,
                                    const std::vector<TermPlan>& plans,
                                    std::vector<uint32_t>* candidates,
                                    std::vector<DocidSource>* sources);

Status filter_docids_by_conjunction(const reader::LogicalIndexReader& idx,
                                    const io::BatchRangeFetcher& round1,
                                    const std::vector<TermPlan>& plans,
                                    const std::vector<uint32_t>& initial_candidates,
                                    std::vector<uint32_t>* candidates,
                                    std::vector<DocidSource>* sources);

} // namespace doris::snii::query::internal
