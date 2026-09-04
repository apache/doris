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
    return open_impl(idx, out, true);
}

#ifdef BE_TEST
Status SniiStatsProvider::open_legacy_for_test(const reader::LogicalIndexReader* idx,
                                               SniiStatsProvider* out) {
    return open_impl(idx, out, false);
}
#endif

Status SniiStatsProvider::open_impl(const reader::LogicalIndexReader* idx, SniiStatsProvider* out,
                                    bool require_semantic_metadata) {
    if (idx == nullptr || out == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("stats_provider: null argument");
    }
    out->idx_ = idx;
    const auto& sb = idx->stats();
    out->doc_count_ = sb.doc_count;
    out->indexed_doc_count_ = sb.indexed_doc_count;
    out->sum_total_term_freq_ = sb.sum_total_term_freq;

    const RegionRef& norms = idx->section_refs().norms;
    const auto* metadata = idx->common_grams_metadata();
    if (metadata != nullptr) {
        using namespace segment_v2::inverted_index;
        RETURN_IF_ERROR(validate_snii_scoring_metadata(
                metadata, sb.doc_count, sb.sum_total_term_freq,
                idx->tier() == format::IndexTier::kT3, idx->has_positions(), norms.length != 0));
        out->doc_count_ = metadata->scoring_doc_count;
        out->indexed_doc_count_ = metadata->scoring_doc_count;
        out->sum_total_term_freq_ = metadata->scoring_token_count;
    } else if (require_semantic_metadata) {
        RETURN_IF_ERROR(segment_v2::inverted_index::validate_snii_scoring_metadata(
                nullptr, sb.doc_count, sb.sum_total_term_freq,
                idx->tier() == format::IndexTier::kT3, idx->has_positions(), norms.length != 0));
    }
    if (norms.length == 0) {
        out->has_norms_ = false;
        return Status::OK();
    }

    RETURN_IF_ERROR(idx->open_norms(&out->norms_reader_));
    if (metadata != nullptr && out->norms_reader_.doc_count() != metadata->scoring_doc_count) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "snii_stats: semantic norms doc count {} differs from scoring doc count {}",
                out->norms_reader_.doc_count(), metadata->scoring_doc_count);
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

Status SniiStatsProvider::total_term_freq(std::string_view term, uint64_t* ttf) const {
    if (ttf == nullptr)
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("stats_provider: null ttf");
    *ttf = 0;
    bool found = false;
    DictEntry entry;
    RETURN_IF_ERROR(lookup_entry(*idx_, term, &found, &entry));
    if (!found) return Status::OK();
    // tier>=T2 entries carry the total term frequency directly in ttf_delta (the
    // LogicalIndexWriter stores ttf there, not a delta from df). G16-f blocks
    // (kNoTermStats: freq-dropped index) omit it -- fail with the semantic
    // error instead of silently returning the default 0 (mixed old/new
    // segments would otherwise disagree on the same term).
    if (!entry.term_stats_present) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "snii_stats: ttf requested but the dict block carries no term stats "
                "(freq-dropped index)");
    }
    *ttf = entry.ttf_delta;
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
