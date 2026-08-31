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

#include <cstddef>
#include <cstdint>
#include <vector>

#include "common/status.h"

namespace doris::snii::format {

// Optional allocation seam for full CSR decode callers that must gate every
// retained/output allocation before touching the physical buffers. Query
// decoders leave this null; native compaction supplies its shared-budget
// workspace. Implementations must leave both CSR buffers unchanged when a
// reservation fails.
class PrxCsrAllocationGate {
public:
    virtual ~PrxCsrAllocationGate() = default;

    virtual Status reserve_csr(std::vector<uint32_t>* pos_flat, size_t position_count,
                               std::vector<uint32_t>* pos_off, size_t offset_count) = 0;
    virtual Status reserve_decompression(size_t bytes, std::vector<uint8_t>** buffer) = 0;
};

struct PrxDecodeStats {
    uint64_t raw_frames = 0;
    uint64_t zstd_frames = 0;
    uint64_t pfor_frames = 0;
    uint64_t plaintext_bytes = 0;
    uint64_t total_docs = 0;
    uint64_t selected_docs = 0;
    uint64_t total_positions = 0;
    uint64_t selected_positions = 0;
    uint64_t fetch_ns = 0;
    // Inclusive successful-frame time: header/CRC validation, optional
    // decompression, and payload decode.
    uint64_t decode_ns = 0;
    // Phrase verification excluding only the inclusive decode_ns delta.
    uint64_t phrase_verify_ns = 0;

    void merge(const PrxDecodeStats& other);
    [[nodiscard]] uint64_t frame_count() const { return raw_frames + zstd_frames + pfor_frames; }
    [[nodiscard]] bool is_valid() const {
        return selected_docs <= total_docs && selected_positions <= total_positions;
    }
    bool operator==(const PrxDecodeStats&) const = default;
};

struct PrxDecodedShape {
    uint32_t total_docs = 0;
    uint64_t total_positions = 0;
    uint32_t max_frequency = 0;
    bool has_zero_frequency = false;
};

// Query-plan and matcher calibration inputs are deliberately separate from PrxDecodeStats: the
// latter's 11 production counters remain a stable decode contract.
struct PhraseQueryExecutionStats {
    uint64_t exact_candidate_docs = 0;
    uint64_t exact_candidate_visits = 0;
    uint64_t prx_streaming_frames = 0;
    uint64_t prefix_leading_candidate_docs = 0;
    uint64_t prefix_tail_candidate_visits = 0;
    uint64_t common_grams_candidate_queries = 0;
    uint64_t common_grams_plain_plans = 0;
    uint64_t common_grams_gram_plans = 0;
    uint64_t common_grams_fallback_no_gram = 0;
    uint64_t common_grams_fallback_incompatible = 0;
    uint64_t common_grams_fallback_kill_switch = 0;
    uint64_t common_grams_fallback_cost = 0;
    uint64_t common_grams_fallback_base_analyzer_mismatch = 0;
    uint64_t common_grams_fallback_prefix_tail_empty = 0;
    uint64_t common_grams_authoritative_empty = 0;
    uint64_t common_grams_plain_posting_bytes = 0;
    uint64_t common_grams_gram_posting_bytes = 0;
    uint64_t common_grams_plain_estimated_candidate_df = 0;
    uint64_t common_grams_gram_estimated_candidate_df = 0;
    uint64_t common_grams_plain_estimated_cost = 0;
    uint64_t common_grams_gram_estimated_cost = 0;
    uint64_t common_grams_planning_ns = 0;
};

struct PrxDecodeContext {
    PrxDecodeStats* stats = nullptr;
    PrxDecodedShape* shape = nullptr;
    PhraseQueryExecutionStats* query_stats = nullptr;
    PrxCsrAllocationGate* allocation_gate = nullptr;
};

} // namespace doris::snii::format
