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
#include <string_view>

#include "common/status.h"
#include "storage/index/snii/format/norms_pod.h"
#include "storage/index/snii/reader/logical_index_reader.h"

// SniiStatsProvider -- exposes the native SNII scoring statistics required by
// BM25, sourced directly from the on-disk structures of one logical index:
//   - semantic segment-level counts from CommonGrams scoring metadata.
//   - per-term df / ttf from the term's DictEntry (resolved through the reader's
//     lookup flow). The LogicalIndexWriter stores ttf directly in ttf_delta for
//     tier>=T2 entries, so total_term_freq returns entry.ttf_delta.
//   - per-doc length normalization byte (encoded_norm) from the norms POD,
//     lazily loaded and validated once by LogicalIndexReader, then shared by
//     every stats provider for that cached logical index.
//
// avgdl() = sum_total_term_freq / max(1, indexed_doc_count): the average document
// length used by BM25 length normalization. The provider performs no scoring; it
// only surfaces the statistics so query::Bm25Scorer can combine them.
namespace doris::snii::stats {

class SniiStatsProvider {
public:
    SniiStatsProvider() = default;

    // Binds to idx and acquires its shared validated norms view when the index
    // carries scoring norms. idx must outlive this provider. Complete CommonGrams
    // scoring metadata requires compatible semantic statistics and norms.
    static Status open(const reader::LogicalIndexReader* idx, SniiStatsProvider* out);

#ifdef BE_TEST
    // Legacy scorer fixtures predate semantic scoring metadata. Production
    // callers must use open() and fail closed when the proof is absent.
    static Status open_legacy_for_test(const reader::LogicalIndexReader* idx,
                                       SniiStatsProvider* out);
#endif

    // Segment-level semantic counts persisted by the SNII writer.
    uint64_t doc_count() const { return doc_count_; }
    uint64_t indexed_doc_count() const { return indexed_doc_count_; }
    uint64_t sum_total_term_freq() const { return sum_total_term_freq_; }

    // Average document length: sum_total_term_freq / max(1, indexed_doc_count).
    double avgdl() const;

    // Per-term document frequency. Absent term -> *df = 0 (OK status).
    Status doc_freq(std::string_view term, uint64_t* df) const;

    // Per-term total term frequency (ttf = df + ttf_delta at tier>=T2). Absent
    // term -> *ttf = 0 (OK status).
    Status total_term_freq(std::string_view term, uint64_t* ttf) const;

    // 1-byte encoded doc-length norm for docid (raw byte from the norms POD).
    // Out-of-range docid -> InvalidArgument; index without norms -> InvalidArgument.
    Status encoded_norm(uint32_t docid, uint8_t* out) const;

    bool has_norms() const { return has_norms_; }

private:
    static Status open_impl(const reader::LogicalIndexReader* idx, SniiStatsProvider* out,
                            bool require_semantic_metadata);

    const reader::LogicalIndexReader* idx_ = nullptr;
    uint64_t doc_count_ = 0;
    uint64_t indexed_doc_count_ = 0;
    uint64_t sum_total_term_freq_ = 0;
    bool has_norms_ = false;
    // Zero-copy view into the immutable bytes owned by idx_ after open_norms().
    format::NormsPodReader norms_reader_;
};

} // namespace doris::snii::stats
