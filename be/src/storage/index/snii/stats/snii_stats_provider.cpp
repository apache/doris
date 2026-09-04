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

#include "storage/index/snii/stats/snii_stats_provider.h"

#include <algorithm>

#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/format/stats_block.h"

namespace doris::snii::stats {

using format::DictEntry;
using format::NormsPodReader;
using format::RegionRef;

namespace {

// Resolves a term's DictEntry. *found=false for an absent term (OK status).
Status lookup_entry(const reader::LogicalIndexReader& idx, std::string_view term, bool* found,
                    DictEntry* entry) {
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    return idx.lookup(term, found, entry, &frq_base, &prx_base);
}

} // namespace

Status SniiStatsProvider::open(const reader::LogicalIndexReader* idx, SniiStatsProvider* out) {
    if (idx == nullptr || out == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("stats_provider: null argument");
    }
    // 统计全部来自物理统计：doc_count / indexed_doc_count / sum_total_term_freq 由 writer 对
    // 每个 posting 累加，avgdl 按注释定义用 indexed_doc_count（不含 NULL 行）。
    out->idx_ = idx;
    const auto& sb = idx->stats();
    out->doc_count_ = sb.doc_count;
    out->indexed_doc_count_ = sb.indexed_doc_count;
    out->sum_total_term_freq_ = sb.sum_total_term_freq;
    const RegionRef& norms = idx->section_refs().norms;
    if (norms.length == 0) {
        out->has_norms_ = false;
        return Status::OK();
    }

    RETURN_IF_ERROR(idx->open_norms(&out->norms_reader_));
    if (out->norms_reader_.doc_count() != sb.doc_count) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "snii_stats: norms doc count {} differs from segment doc count {}",
                out->norms_reader_.doc_count(), sb.doc_count);
    }
    out->has_norms_ = true;
    return Status::OK();
}

double SniiStatsProvider::avgdl() const {
    const uint64_t denom = std::max<uint64_t>(1, indexed_doc_count_);
    return static_cast<double>(sum_total_term_freq_) / static_cast<double>(denom);
}

Status SniiStatsProvider::doc_freq(std::string_view term, uint64_t* df) const {
    if (df == nullptr)
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("stats_provider: null df");
    *df = 0;
    bool found = false;
    DictEntry entry;
    RETURN_IF_ERROR(lookup_entry(*idx_, term, &found, &entry));
    if (found) *df = entry.df;
    return Status::OK();
}

Status SniiStatsProvider::encoded_norm(uint32_t docid, uint8_t* out) const {
    if (out == nullptr)
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("stats_provider: null out");
    if (!has_norms_) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "stats_provider: index has no norms");
    }
    return norms_reader_.try_encoded_norm(docid, out);
}

} // namespace doris::snii::stats
