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
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/compaction/eligibility.h"
#include "storage/index/snii/compaction/posting_cursor.h"
#include "storage/index/snii/compaction/rowid_conversion.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/writer/memory_reporter.h"
#include "storage/index/snii/writer/snii_compound_writer.h"

namespace doris::snii::compaction {

class TermMergeFrontier;

// Prepared, one-shot merge of one plain positions-only logical index across
// source segments. prepare() performs every O(1)-metadata, row-id and NULL
// preflight before the caller creates destination streamed sessions. execute()
// then performs exactly one dictionary/posting pass and seals every session.
//
// Source readers and the validated row-id conversion token are borrowed and
// must remain stable through execute(). The plan owns the per-source read
// contexts, destination row counts and remapped NULL docids. Its aggregate
// read-ahead allocation never exceeds the explicit prepare() budget.
class SniiPlainT2MergePlan {
public:
    // Below this aggregate-per-source budget, two posting streams would issue
    // tiny range reads and turn a memory fallback into an IO regression.
    static constexpr size_t kMinReadAheadBudgetPerSource = 64U << 10;

    SniiPlainT2MergePlan(const SniiPlainT2MergePlan&) = delete;
    SniiPlainT2MergePlan& operator=(const SniiPlainT2MergePlan&) = delete;
    SniiPlainT2MergePlan(SniiPlainT2MergePlan&&) = delete;
    SniiPlainT2MergePlan& operator=(SniiPlainT2MergePlan&&) = delete;

    static Status prepare(std::vector<const reader::LogicalIndexReader*> source_indexes,
                          const ValidatedRowIdConversion& rowid_conversion,
                          size_t total_read_ahead_budget_bytes,
                          std::unique_ptr<SniiPlainT2MergePlan>* out);
    static Status prepare(std::vector<const reader::LogicalIndexReader*> source_indexes,
                          const ValidatedRowIdConversion& rowid_conversion,
                          size_t total_read_ahead_budget_bytes,
                          std::shared_ptr<writer::MemoryReporter> memory_reporter,
                          std::unique_ptr<SniiPlainT2MergePlan>* out);
    static Status prepare(std::vector<const reader::LogicalIndexReader*> source_indexes,
                          const ValidatedRowIdConversion& rowid_conversion,
                          const SniiCompactionEligibility& eligibility,
                          size_t total_read_ahead_budget_bytes,
                          std::unique_ptr<SniiPlainT2MergePlan>* out);
    static Status prepare(std::vector<const reader::LogicalIndexReader*> source_indexes,
                          const ValidatedRowIdConversion& rowid_conversion,
                          const SniiCompactionEligibility& eligibility,
                          size_t total_read_ahead_budget_bytes,
                          std::shared_ptr<writer::MemoryReporter> memory_reporter,
                          std::unique_ptr<SniiPlainT2MergePlan>* out);

    const std::vector<uint32_t>& destination_null_docids(size_t destination_segment) const;
    writer::TrackedNullDocids take_destination_null_docids(size_t destination_segment);
    const std::vector<uint8_t>& destination_encoded_norms(size_t destination_segment) const;
    writer::TrackedEncodedNorms take_destination_encoded_norms(size_t destination_segment);
    format::IndexConfig destination_index_config() const;
    std::optional<segment_v2::inverted_index::CommonGramsSegmentMetadata>
    destination_common_grams_metadata(size_t destination_segment) const;
    format::CommonGramsPostingPolicy destination_common_grams_posting_policy() const {
        return eligibility_.common_grams_posting_policy;
    }
    size_t destination_segment_count() const { return destination_segment_num_rows_.size(); }

    // Sessions must correspond one-for-one with destination segments and must
    // already carry the doc_count and destination_null_docids() returned by this
    // plan. Any failure is terminal: the first error is sticky, sessions remain
    // unsealable, and the whole unpublished compaction output must be discarded.
    Status execute(std::span<writer::SniiStreamedIndexSession* const> sessions);

private:
    struct CurrentTerm {
        std::string term;
        std::vector<std::unique_ptr<SniiPostingCursor>> posting_cursors;
        std::optional<bool> has_positions;
        bool common_gram = false;
        bool counts_as_semantic_token = false;
    };

    SniiPlainT2MergePlan(
            std::vector<const reader::LogicalIndexReader*> source_indexes,
            const ValidatedRowIdConversion* rowid_conversion,
            std::vector<uint32_t> destination_segment_num_rows,
            std::vector<writer::MemoryReporter::Reservation> destination_null_reservations,
            std::vector<std::vector<uint32_t>> destination_null_docids,
            SniiCompactionEligibility eligibility,
            std::vector<writer::MemoryReporter::Reservation> destination_norm_reservations,
            std::vector<std::vector<uint8_t>> destination_encoded_norms,
            std::shared_ptr<writer::MemoryReporter> memory_reporter,
            std::vector<std::unique_ptr<SniiPostingReadContext>> read_contexts);

    Status take_front_source(TermMergeFrontier* frontier, CurrentTerm* current);
    Status take_current_term(TermMergeFrontier* frontier, CurrentTerm* current);
    Status write_current_term(CurrentTerm current,
                              std::span<writer::SniiStreamedIndexSession* const> sessions);
    Status merge_terms(std::span<writer::SniiStreamedIndexSession* const> sessions);
    Status poison(Status status);

    std::vector<const reader::LogicalIndexReader*> source_indexes_;
    const ValidatedRowIdConversion* rowid_conversion_ = nullptr;
    std::vector<uint32_t> destination_segment_num_rows_;
    std::shared_ptr<writer::MemoryReporter> memory_reporter_;
    // Reservations precede their vectors so physical memory is destroyed first.
    std::vector<writer::MemoryReporter::Reservation> destination_null_reservations_;
    std::vector<std::vector<uint32_t>> destination_null_docids_;
    std::vector<bool> destination_null_docids_taken_;
    SniiCompactionEligibility eligibility_;
    // Reservations precede their vectors so physical memory is destroyed first.
    std::vector<writer::MemoryReporter::Reservation> destination_norm_reservations_;
    std::vector<std::vector<uint8_t>> destination_encoded_norms_;
    std::vector<bool> destination_encoded_norms_taken_;
    std::vector<uint64_t> destination_semantic_token_counts_;
    std::vector<std::unique_ptr<SniiPostingReadContext>> read_contexts_;
    bool executed_ = false;
    Status failed_ = Status::OK();
};

} // namespace doris::snii::compaction
