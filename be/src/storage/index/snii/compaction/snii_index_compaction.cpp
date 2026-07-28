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

#include "storage/index/snii/compaction/snii_index_compaction.h"

#include <algorithm>
#include <limits>
#include <memory>
#include <string_view>
#include <tuple>
#include <utility>

#include "common/check.h"
#include "common/logging.h"
#include "storage/index/inverted/common_grams/common_grams_key_codec.h"
#include "storage/index/snii/compaction/eligibility.h"
#include "storage/index/snii/compaction/posting_run_merger.h"
#include "storage/index/snii/compaction/term_cursor.h"
#include "storage/index/snii/compaction/term_merge_frontier.h"
#include "storage/index/snii/format/norms_pod.h"
#include "storage/index/snii/writer/spimi_term_buffer.h"

namespace doris::snii::compaction {

namespace {

Status invalid_plan(std::string_view reason) {
    return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("snii_compaction: {}", reason);
}

Status merge_corruption(std::string_view reason) {
    return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("snii_compaction: {}",
                                                                          reason);
}

bool is_well_formed_common_gram(std::string_view term) {
    namespace inverted_index = segment_v2::inverted_index;
    if (!term.starts_with(inverted_index::CG_V1_MARKER) ||
        term.size() > inverted_index::COMMON_GRAM_MAX_ENCODED_BYTES) {
        return false;
    }
    constexpr size_t kLengthBytes = 8;
    const size_t length_offset = inverted_index::CG_V1_MARKER.size();
    if (term.size() < length_offset + kLengthBytes + 1) {
        return false;
    }
    uint32_t left_length = 0;
    for (size_t i = 0; i < kLengthBytes; ++i) {
        const char digit = term[length_offset + i];
        if (!((digit >= '0' && digit <= '9') || (digit >= 'a' && digit <= 'f'))) {
            return false;
        }
        left_length = (left_length << 4) |
                      static_cast<uint32_t>(digit <= '9' ? digit - '0' : digit - 'a' + 10);
    }
    const size_t separator = length_offset + kLengthBytes;
    if (term[separator] != ':') {
        return false;
    }
    const std::string_view components = term.substr(separator + 1);
    if (left_length > components.size()) {
        return false;
    }
    return inverted_index::validate_common_grams_logical_term(components.substr(0, left_length),
                                                              "left term")
                   .ok() &&
           inverted_index::validate_common_grams_logical_term(components.substr(left_length),
                                                              "right term")
                   .ok();
}

template <typename T>
Status reserve_tracked_vector(std::vector<T>* values, size_t additional,
                              writer::MemoryReporter::Reservation* reservation) {
    if (reservation == nullptr || additional == 0) return Status::OK();
    if (additional > std::numeric_limits<size_t>::max() - values->size()) {
        return Status::Error<ErrorCode::MEM_LIMIT_EXCEEDED, false>(
                "snii_compaction: destination posting vector size overflows");
    }
    const size_t required = values->size() + additional;
    if (required <= values->capacity()) {
        DCHECK_EQ(reservation->bytes(), values->capacity() * sizeof(T));
        return Status::OK();
    }
    size_t target = std::max<size_t>(64, required);
    if (values->capacity() != 0 && values->capacity() <= std::numeric_limits<size_t>::max() / 2) {
        target = std::max(target, values->capacity() * 2);
    }
    if (target > static_cast<size_t>(std::numeric_limits<int64_t>::max()) / sizeof(T)) {
        return Status::Error<ErrorCode::MEM_LIMIT_EXCEEDED, false>(
                "snii_compaction: destination posting reservation exceeds int64");
    }
    writer::MemoryReporter::Reservation replacement;
    RETURN_IF_ERROR(reservation->prepare_replacement(target * sizeof(T), &replacement));
    values->reserve(target);
    DCHECK_EQ(values->capacity(), target);
    *reservation = std::move(replacement);
    return Status::OK();
}
} // namespace

SniiPlainT2MergePlan::SniiPlainT2MergePlan(
        std::vector<const reader::LogicalIndexReader*> source_indexes,
        const ValidatedRowIdConversion* rowid_conversion,
        std::vector<uint32_t> destination_segment_num_rows,
        std::vector<writer::MemoryReporter::Reservation> destination_null_reservations,
        std::vector<std::vector<uint32_t>> destination_null_docids,
        SniiCompactionEligibility eligibility,
        std::vector<writer::MemoryReporter::Reservation> destination_norm_reservations,
        std::vector<std::vector<uint8_t>> destination_encoded_norms,
        std::shared_ptr<writer::MemoryReporter> memory_reporter,
        std::vector<std::unique_ptr<SniiPostingReadContext>> read_contexts)
        : source_indexes_(std::move(source_indexes)),
          rowid_conversion_(rowid_conversion),
          destination_segment_num_rows_(std::move(destination_segment_num_rows)),
          memory_reporter_(std::move(memory_reporter)),
          destination_null_reservations_(std::move(destination_null_reservations)),
          destination_null_docids_(std::move(destination_null_docids)),
          destination_null_docids_taken_(destination_null_docids_.size(), false),
          eligibility_(std::move(eligibility)),
          destination_norm_reservations_(std::move(destination_norm_reservations)),
          destination_encoded_norms_(std::move(destination_encoded_norms)),
          destination_encoded_norms_taken_(destination_encoded_norms_.size(), false),
          destination_semantic_token_counts_(destination_segment_num_rows_.size(), 0),
          read_contexts_(std::move(read_contexts)) {}

Status SniiPlainT2MergePlan::prepare(std::vector<const reader::LogicalIndexReader*> source_indexes,
                                     const ValidatedRowIdConversion& rowid_conversion,
                                     size_t total_read_ahead_budget_bytes,
                                     std::unique_ptr<SniiPlainT2MergePlan>* out) {
    return prepare(std::move(source_indexes), rowid_conversion, total_read_ahead_budget_bytes,
                   nullptr, out);
}

Status SniiPlainT2MergePlan::prepare(std::vector<const reader::LogicalIndexReader*> source_indexes,
                                     const ValidatedRowIdConversion& rowid_conversion,
                                     size_t total_read_ahead_budget_bytes,
                                     std::shared_ptr<writer::MemoryReporter> memory_reporter,
                                     std::unique_ptr<SniiPlainT2MergePlan>* out) {
    SniiCompactionEligibility eligibility;
    eligibility.kind = SniiStreamedMergeKind::kPlainT2;
    return prepare(std::move(source_indexes), rowid_conversion, eligibility,
                   total_read_ahead_budget_bytes, std::move(memory_reporter), out);
}

Status SniiPlainT2MergePlan::prepare(std::vector<const reader::LogicalIndexReader*> source_indexes,
                                     const ValidatedRowIdConversion& rowid_conversion,
                                     const SniiCompactionEligibility& eligibility,
                                     size_t total_read_ahead_budget_bytes,
                                     std::unique_ptr<SniiPlainT2MergePlan>* out) {
    return prepare(std::move(source_indexes), rowid_conversion, eligibility,
                   total_read_ahead_budget_bytes, nullptr, out);
}

Status SniiPlainT2MergePlan::prepare(std::vector<const reader::LogicalIndexReader*> source_indexes,
                                     const ValidatedRowIdConversion& rowid_conversion,
                                     const SniiCompactionEligibility& eligibility,
                                     size_t total_read_ahead_budget_bytes,
                                     std::shared_ptr<writer::MemoryReporter> memory_reporter,
                                     std::unique_ptr<SniiPlainT2MergePlan>* out) {
    if (out == nullptr) {
        return invalid_plan("null plan out parameter");
    }
    out->reset();
    if (source_indexes.empty()) {
        return invalid_plan("no source indexes");
    }
    const std::vector<uint32_t>& destination_segment_num_rows =
            rowid_conversion.destination_segment_doc_counts();
    if (destination_segment_num_rows.empty()) {
        return invalid_plan("no destination segments");
    }
    if (source_indexes.size() != rowid_conversion.source_segment_count()) {
        return invalid_plan("source index count differs from row-id conversion");
    }
    // Division first avoids source_count * minimum overflow. Rejecting tiny
    // allocations is an IO gate: the raw rebuild is cheaper than issuing a
    // range request for every small posting window.
    if (source_indexes.size() > total_read_ahead_budget_bytes / kMinReadAheadBudgetPerSource) {
        return invalid_plan("read-ahead budget is below the per-source IO floor");
    }
    const size_t per_source_read_ahead_budget =
            std::min(SniiPostingReadContext::kMaxReadAheadBudgetBytes,
                     total_read_ahead_budget_bytes / source_indexes.size());
    DORIS_CHECK_GE(per_source_read_ahead_budget, kMinReadAheadBudgetPerSource);
    DORIS_CHECK_LE(per_source_read_ahead_budget,
                   total_read_ahead_budget_bytes / source_indexes.size());

    for (size_t source_ordinal = 0; source_ordinal < source_indexes.size(); ++source_ordinal) {
        const reader::LogicalIndexReader* source = source_indexes[source_ordinal];
        if (source == nullptr) {
            return invalid_plan("null source index");
        }
        RETURN_IF_ERROR(validate_snii_source_eligibility(*source, source_ordinal, eligibility));
        if (source->stats().doc_count > std::numeric_limits<uint32_t>::max()) {
            return merge_corruption("source doc count exceeds the SNII uint32 docid domain");
        }
        if (source->stats().doc_count !=
            rowid_conversion.source_segment_doc_counts()[source_ordinal]) {
            return invalid_plan("source doc count differs from validated row-id conversion");
        }
    }

    std::vector<writer::MemoryReporter::Reservation> destination_null_reservations;
    destination_null_reservations.reserve(destination_segment_num_rows.size());
    for (size_t destination_ordinal = 0; destination_ordinal < destination_segment_num_rows.size();
         ++destination_ordinal) {
        destination_null_reservations.push_back(memory_reporter == nullptr
                                                        ? writer::MemoryReporter::Reservation()
                                                        : memory_reporter->make_reservation());
    }
    std::vector<std::vector<uint32_t>> destination_null_docids(destination_segment_num_rows.size());
    for (size_t source_ordinal = 0; source_ordinal < source_indexes.size(); ++source_ordinal) {
        reader::NullDocidsScanMemory scan_memory;
        RETURN_IF_ERROR(source_indexes[source_ordinal]->null_docids_scan_memory(&scan_memory));
        writer::MemoryReporter::Reservation source_output_reservation =
                memory_reporter == nullptr ? writer::MemoryReporter::Reservation()
                                           : memory_reporter->make_reservation();
        writer::MemoryReporter::Reservation source_frame_reservation =
                memory_reporter == nullptr ? writer::MemoryReporter::Reservation()
                                           : memory_reporter->make_reservation();
        writer::MemoryReporter::Reservation source_decode_reservation =
                memory_reporter == nullptr ? writer::MemoryReporter::Reservation()
                                           : memory_reporter->make_reservation();
        if (memory_reporter != nullptr) {
            RETURN_IF_ERROR(source_output_reservation.set_bytes(scan_memory.output_bytes));
            RETURN_IF_ERROR(source_frame_reservation.set_bytes(scan_memory.frame_bytes));
        }
        std::vector<uint32_t> source_null_docids;
        source_null_docids.reserve(source_indexes[source_ordinal]->stats().null_count);
        if (memory_reporter != nullptr) {
            DORIS_CHECK_EQ(source_null_docids.capacity() * sizeof(uint32_t),
                           source_output_reservation.bytes());
        }
        RETURN_IF_ERROR(source_indexes[source_ordinal]->read_null_docids(
                &source_null_docids, [&](uint64_t bytes) {
                    return memory_reporter == nullptr ? Status::OK()
                                                      : source_decode_reservation.set_bytes(bytes);
                }));
        source_frame_reservation.reset();
        source_decode_reservation.reset();
        const auto source_mapping = rowid_conversion.source_mapping(source_ordinal);
        for (uint32_t source_docid : source_null_docids) {
            DCHECK_LT(source_docid, source_mapping.size());
            const auto [destination_segment, destination_docid] = source_mapping[source_docid];
            const bool segment_deleted =
                    destination_segment == std::numeric_limits<uint32_t>::max();
            const bool doc_deleted = destination_docid == std::numeric_limits<uint32_t>::max();
            DCHECK_EQ(segment_deleted, doc_deleted);
            if (!segment_deleted) {
                DCHECK_LT(destination_segment, destination_null_docids.size());
                DCHECK_LT(destination_docid, destination_segment_num_rows[destination_segment]);
                RETURN_IF_ERROR(reserve_tracked_vector(
                        &destination_null_docids[destination_segment], 1,
                        memory_reporter == nullptr
                                ? nullptr
                                : &destination_null_reservations[destination_segment]));
                destination_null_docids[destination_segment].push_back(destination_docid);
            }
        }
    }
    for (auto& null_docids : destination_null_docids) {
        std::ranges::sort(null_docids);
        DCHECK(std::adjacent_find(null_docids.begin(), null_docids.end()) == null_docids.end());
    }

    std::vector<writer::MemoryReporter::Reservation> destination_norm_reservations;
    std::vector<std::vector<uint8_t>> destination_encoded_norms;
    if (eligibility.kind == SniiStreamedMergeKind::kCommonGramsT3) {
        DORIS_CHECK(eligibility.common_grams_metadata_seed.has_value());
        destination_norm_reservations.reserve(destination_segment_num_rows.size());
        destination_encoded_norms.resize(destination_segment_num_rows.size());
        for (size_t destination_ordinal = 0;
             destination_ordinal < destination_segment_num_rows.size(); ++destination_ordinal) {
            destination_norm_reservations.push_back(memory_reporter == nullptr
                                                            ? writer::MemoryReporter::Reservation()
                                                            : memory_reporter->make_reservation());
            const uint32_t doc_count = destination_segment_num_rows[destination_ordinal];
            auto& norms = destination_encoded_norms[destination_ordinal];
            RETURN_IF_ERROR(reserve_tracked_vector(
                    &norms, doc_count,
                    memory_reporter == nullptr ? nullptr : &destination_norm_reservations.back()));
            norms.resize(doc_count);
            if (memory_reporter != nullptr) {
                DORIS_CHECK_EQ(destination_norm_reservations.back().bytes(), norms.capacity());
            }
        }
        for (size_t source_ordinal = 0; source_ordinal < source_indexes.size(); ++source_ordinal) {
            writer::MemoryReporter::Reservation source_norms_reservation =
                    memory_reporter == nullptr ? writer::MemoryReporter::Reservation()
                                               : memory_reporter->make_reservation();
            if (memory_reporter != nullptr) {
                RETURN_IF_ERROR(source_norms_reservation.set_bytes(
                        source_indexes[source_ordinal]->compaction_norms_cache_charge()));
            }
            format::NormsPodReader source_norms;
            RETURN_IF_ERROR(source_indexes[source_ordinal]->open_norms(&source_norms));
            const auto source_mapping = rowid_conversion.source_mapping(source_ordinal);
            // The norms POD is only CRC-self-consistent; nothing upstream ties its
            // doc_count to the validated conversion, and the loop below indexes
            // source_mapping by it. Reconcile loudly (once per source, cold path).
            if (source_norms.doc_count() != source_mapping.size()) {
                return merge_corruption("norms doc count differs from validated row-id conversion");
            }
            for (uint32_t source_docid = 0; source_docid < source_norms.doc_count();
                 ++source_docid) {
                const auto [destination_segment, destination_docid] = source_mapping[source_docid];
                const bool deleted = destination_segment == std::numeric_limits<uint32_t>::max();
                DCHECK_EQ(deleted, destination_docid == std::numeric_limits<uint32_t>::max());
                if (deleted) {
                    continue;
                }
                DCHECK_LT(destination_segment, destination_encoded_norms.size());
                DCHECK_LT(destination_docid, destination_encoded_norms[destination_segment].size());
                destination_encoded_norms[destination_segment][destination_docid] =
                        source_norms.encoded_norm(source_docid);
            }
            source_indexes[source_ordinal]->release_compaction_norms();
            source_norms_reservation.reset();
        }
    }

    std::vector<std::unique_ptr<SniiPostingReadContext>> read_contexts;
    read_contexts.reserve(source_indexes.size());
    for (const reader::LogicalIndexReader* source : source_indexes) {
        auto context = std::make_unique<SniiPostingReadContext>(
                source, per_source_read_ahead_budget, memory_reporter.get());
        RETURN_IF_ERROR(context->init());
        read_contexts.push_back(std::move(context));
    }

    out->reset(new SniiPlainT2MergePlan(
            std::move(source_indexes), &rowid_conversion, destination_segment_num_rows,
            std::move(destination_null_reservations), std::move(destination_null_docids),
            eligibility, std::move(destination_norm_reservations),
            std::move(destination_encoded_norms), std::move(memory_reporter),
            std::move(read_contexts)));
    return Status::OK();
}

const std::vector<uint32_t>& SniiPlainT2MergePlan::destination_null_docids(
        size_t destination_segment) const {
    DORIS_CHECK_LT(destination_segment, destination_null_docids_.size());
    return destination_null_docids_[destination_segment];
}

writer::TrackedNullDocids SniiPlainT2MergePlan::take_destination_null_docids(
        size_t destination_segment) {
    DORIS_CHECK_LT(destination_segment, destination_null_docids_.size());
    DORIS_CHECK(!destination_null_docids_taken_[destination_segment]);
    destination_null_docids_taken_[destination_segment] = true;
    return writer::TrackedNullDocids(std::move(destination_null_reservations_[destination_segment]),
                                     std::move(destination_null_docids_[destination_segment]));
}

const std::vector<uint8_t>& SniiPlainT2MergePlan::destination_encoded_norms(
        size_t destination_segment) const {
    DORIS_CHECK(eligibility_.kind == SniiStreamedMergeKind::kCommonGramsT3);
    DORIS_CHECK_LT(destination_segment, destination_encoded_norms_.size());
    return destination_encoded_norms_[destination_segment];
}

writer::TrackedEncodedNorms SniiPlainT2MergePlan::take_destination_encoded_norms(
        size_t destination_segment) {
    DORIS_CHECK(eligibility_.kind == SniiStreamedMergeKind::kCommonGramsT3);
    DORIS_CHECK_LT(destination_segment, destination_encoded_norms_.size());
    DORIS_CHECK(!destination_encoded_norms_taken_[destination_segment]);
    destination_encoded_norms_taken_[destination_segment] = true;
    return writer::TrackedEncodedNorms(
            std::move(destination_norm_reservations_[destination_segment]),
            std::move(destination_encoded_norms_[destination_segment]));
}

format::IndexConfig SniiPlainT2MergePlan::destination_index_config() const {
    return eligibility_.kind == SniiStreamedMergeKind::kCommonGramsT3
                   ? format::IndexConfig::kDocsPositionsScoring
                   : format::IndexConfig::kDocsPositions;
}

std::optional<segment_v2::inverted_index::CommonGramsSegmentMetadata>
SniiPlainT2MergePlan::destination_common_grams_metadata(size_t destination_segment) const {
    if (eligibility_.kind == SniiStreamedMergeKind::kPlainT2) {
        return std::nullopt;
    }
    DORIS_CHECK(eligibility_.common_grams_metadata_seed.has_value());
    DORIS_CHECK_LT(destination_segment, destination_segment_num_rows_.size());
    auto metadata = *eligibility_.common_grams_metadata_seed;
    metadata.scoring_doc_count = destination_segment_num_rows_[destination_segment];
    metadata.scoring_token_count = 0;
    return metadata;
}

Status SniiPlainT2MergePlan::poison(Status status) {
    DORIS_CHECK(!status.ok());
    if (failed_.ok()) {
        failed_ = std::move(status);
    }
    return failed_;
}

Status SniiPlainT2MergePlan::take_front_source(TermMergeFrontier* frontier, CurrentTerm* current) {
    SniiSegmentTermCursor* source = frontier->front();
    const uint32_t source_ordinal = source->source_ordinal();
    DCHECK_LT(source_ordinal, source_indexes_.size());
    const uint64_t frq_base = source->frq_base();
    const uint64_t prx_base = source->prx_base();
    format::DictEntry entry = source->take_entry();
    if (current->posting_cursors.empty()) {
        current->term = std::move(entry.term);
    }

    const bool source_has_positions = posting_entry_has_positions(entry);
    if (!current->common_gram && !source_has_positions) {
        return merge_corruption("ordinary term has docs-only posting shape");
    }
    if (!current->has_positions.has_value()) {
        current->has_positions = source_has_positions;
    } else if (*current->has_positions != source_has_positions) {
        return merge_corruption("same term has inconsistent position shape across sources");
    }

    auto cursor = std::make_unique<SniiPostingCursor>(read_contexts_[source_ordinal].get(),
                                                      std::move(entry), frq_base, prx_base,
                                                      source_ordinal, rowid_conversion_);
    RETURN_IF_ERROR(cursor->init());
    current->posting_cursors.push_back(std::move(cursor));
    return frontier->advance_front();
}

Status SniiPlainT2MergePlan::take_current_term(TermMergeFrontier* frontier, CurrentTerm* current) {
    DCHECK(frontier != nullptr);
    DCHECK(current != nullptr);
    DCHECK(!frontier->empty());
    const std::string_view group_term = frontier->front()->term();
    current->posting_cursors.reserve(source_indexes_.size());
    if (eligibility_.kind == SniiStreamedMergeKind::kCommonGramsT3) {
        current->common_gram = segment_v2::inverted_index::is_internal_term_key(group_term);
        if (current->common_gram && !is_well_formed_common_gram(group_term)) {
            return merge_corruption("CommonGrams source contains an unknown internal term marker");
        }
        current->counts_as_semantic_token = !current->common_gram;
    }

    do {
        RETURN_IF_ERROR(take_front_source(frontier, current));
    } while (!frontier->empty() && frontier->front()->term() == current->term);
    DCHECK(current->has_positions.has_value());
    return Status::OK();
}

Status SniiPlainT2MergePlan::write_current_term(
        CurrentTerm current, std::span<writer::SniiStreamedIndexSession* const> sessions) {
    MergedPostingRuns posting_source(std::move(current.posting_cursors), *current.has_positions,
                                     current.counts_as_semantic_token,
                                     destination_segment_num_rows_,
                                     destination_semantic_token_counts_);
    RETURN_IF_ERROR(posting_source.init());
    while (!posting_source.empty()) {
        const uint32_t destination = posting_source.next_destination();
        DCHECK_LT(destination, sessions.size());
        RETURN_IF_ERROR(posting_source.begin_destination(destination));
        writer::StreamedTermPostings postings {.term = current.term,
                                               .retain_positions = *current.has_positions,
                                               .source = &posting_source};
        RETURN_IF_ERROR(sessions[destination]->push_term(std::move(postings)));
    }
    return Status::OK();
}

Status SniiPlainT2MergePlan::merge_terms(
        std::span<writer::SniiStreamedIndexSession* const> sessions) {
    std::vector<std::unique_ptr<SniiSegmentTermCursor>> term_cursors;
    std::vector<SniiSegmentTermCursor*> term_cursor_ptrs;
    term_cursors.reserve(source_indexes_.size());
    term_cursor_ptrs.reserve(source_indexes_.size());
    for (size_t source_ordinal = 0; source_ordinal < source_indexes_.size(); ++source_ordinal) {
        term_cursors.push_back(std::make_unique<SniiSegmentTermCursor>(
                source_indexes_[source_ordinal], static_cast<uint32_t>(source_ordinal),
                memory_reporter_.get()));
        term_cursor_ptrs.push_back(term_cursors.back().get());
    }
    TermMergeFrontier term_frontier;
    RETURN_IF_ERROR(term_frontier.init(std::move(term_cursor_ptrs)));

    while (!term_frontier.empty()) {
        CurrentTerm current;
        RETURN_IF_ERROR(take_current_term(&term_frontier, &current));
        RETURN_IF_ERROR(write_current_term(std::move(current), sessions));
    }

    if (eligibility_.kind == SniiStreamedMergeKind::kCommonGramsT3) {
        for (size_t destination_ordinal = 0; destination_ordinal < sessions.size();
             ++destination_ordinal) {
            RETURN_IF_ERROR(sessions[destination_ordinal]->set_semantic_token_count(
                    destination_semantic_token_counts_[destination_ordinal]));
        }
    }
    for (writer::SniiStreamedIndexSession* session : sessions) {
        RETURN_IF_ERROR(session->finish());
    }
    return Status::OK();
}

Status SniiPlainT2MergePlan::execute(std::span<writer::SniiStreamedIndexSession* const> sessions) {
    const auto abort_sessions = [&sessions](const Status& cause) {
        DCHECK(!cause.ok());
        for (writer::SniiStreamedIndexSession* session : sessions) {
            if (session != nullptr) {
                session->abort(cause);
            }
        }
    };
    if (!failed_.ok()) {
        abort_sessions(failed_);
        return failed_;
    }
    if (executed_) {
        const Status status = invalid_plan("merge plan executed twice");
        abort_sessions(status);
        return poison(status);
    }
    if (sessions.size() != destination_segment_num_rows_.size()) {
        const Status status = invalid_plan("destination session count mismatch");
        abort_sessions(status);
        return poison(status);
    }
    for (writer::SniiStreamedIndexSession* session : sessions) {
        if (session == nullptr) {
            const Status status = invalid_plan("null destination session");
            abort_sessions(status);
            return poison(status);
        }
    }
    executed_ = true;
    const Status status = merge_terms(sessions);
    if (!status.ok()) {
        abort_sessions(status);
        return poison(status);
    }
    return Status::OK();
}

} // namespace doris::snii::compaction
