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

#include "storage/index/snii/compaction/posting_cursor.h"

#include <fmt/format.h>

#include <algorithm>
#include <limits>
#include <string>
#include <utility>

#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/format/frq_pod.h"
#include "storage/index/snii/format/prx_pod.h"
#include "storage/index/snii/reader/windowed_posting.h"

namespace doris::snii::compaction {

namespace {

Status checked_add(uint64_t lhs, uint64_t rhs, const char* message, uint64_t* out) {
    if (rhs > std::numeric_limits<uint64_t>::max() - lhs) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(message);
    }
    *out = lhs + rhs;
    return Status::OK();
}

Status posting_corruption(const char* message, uint32_t source_ordinal) {
    return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
            "posting_cursor: {} (src_ord={})", message, source_ordinal);
}

} // namespace

Status validate_posting_region(const format::RegionRef& region, uint64_t file_size) {
    if (region.offset > file_size || region.length > file_size - region.offset) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "posting_cursor: posting region outside source file");
    }
    return Status::OK();
}

bool posting_entry_has_positions(const format::DictEntry& entry) {
    return entry.kind == format::DictEntryKind::kInline ? !entry.prx_bytes.empty()
                                                        : entry.prx_len != 0;
}

SniiPostingReadContext::TermLease::~TermLease() {
    if (context_ != nullptr) {
        context_->release_term();
    }
}

size_t SniiPostingReadContext::DecoderWorkspace::capacity_bytes() const {
    return docs_scratch.capacity() + prx_scratch.capacity() + decompressed.capacity() +
           sizeof(uint32_t) * (docids.capacity() + positions_flat.capacity() +
                               position_offsets.capacity() + frequencies.capacity()) +
           sizeof(DestinationPostingRun) * destination_runs.capacity();
}

void SniiPostingReadContext::DecoderWorkspace::init_memory_reporter(
        writer::MemoryReporter* memory_reporter) {
    if (memory_reporter == nullptr) return;
    docs_scratch_reservation = memory_reporter->make_reservation();
    prx_scratch_reservation = memory_reporter->make_reservation();
    docids_reservation = memory_reporter->make_reservation();
    positions_reservation = memory_reporter->make_reservation();
    position_offsets_reservation = memory_reporter->make_reservation();
    destination_runs_reservation = memory_reporter->make_reservation();
    frequencies_reservation = memory_reporter->make_reservation();
    decompressed_reservation = memory_reporter->make_reservation();
    reservations_enabled = true;
}

Status SniiPostingReadContext::DecoderWorkspace::reserve_remapped(size_t document_count,
                                                                  size_t run_count,
                                                                  bool retain_frequencies) {
    const bool grow_runs = destination_runs.capacity() < run_count;
    const bool grow_frequencies = retain_frequencies && frequencies.capacity() < document_count;
    if (!reservations_enabled) {
        destination_runs.reserve(run_count);
        if (retain_frequencies) {
            frequencies.reserve(document_count);
        }
        return Status::OK();
    }
    if (!grow_runs && !grow_frequencies) {
        DCHECK_EQ(destination_runs_reservation.bytes(),
                  destination_runs.capacity() * sizeof(DestinationPostingRun));
        DCHECK_EQ(frequencies_reservation.bytes(), frequencies.capacity() * sizeof(uint32_t));
        return Status::OK();
    }
    if (run_count > std::numeric_limits<size_t>::max() / sizeof(DestinationPostingRun) ||
        document_count > std::numeric_limits<size_t>::max() / sizeof(uint32_t)) {
        return Status::Error<ErrorCode::MEM_LIMIT_EXCEEDED, false>(
                "posting cursor: remapped workspace size overflows size_t");
    }
    writer::MemoryReporter::Reservation replacement_runs;
    writer::MemoryReporter::Reservation replacement_frequencies;
    if (grow_runs) {
        RETURN_IF_ERROR(destination_runs_reservation.prepare_replacement(
                run_count * sizeof(DestinationPostingRun), &replacement_runs));
    }
    if (grow_frequencies) {
        RETURN_IF_ERROR(frequencies_reservation.prepare_replacement(
                document_count * sizeof(uint32_t), &replacement_frequencies));
    }
    {
        std::vector<DestinationPostingRun> new_runs;
        std::vector<uint32_t> new_frequencies;
        if (grow_runs) {
            new_runs.reserve(run_count);
            DCHECK_EQ(new_runs.capacity(), run_count);
            destination_runs.swap(new_runs);
        }
        if (grow_frequencies) {
            new_frequencies.reserve(document_count);
            DCHECK_EQ(new_frequencies.capacity(), document_count);
            frequencies.swap(new_frequencies);
        }
    }
    if (grow_runs) {
        destination_runs_reservation = std::move(replacement_runs);
    }
    if (grow_frequencies) {
        frequencies_reservation = std::move(replacement_frequencies);
    }
    return Status::OK();
}

Status SniiPostingReadContext::DecoderWorkspace::reserve_docids(size_t count) {
    if (!reservations_enabled || docids.capacity() >= count) {
        if (reservations_enabled) {
            DCHECK_EQ(docids_reservation.bytes(), docids.capacity() * sizeof(uint32_t));
        }
        return Status::OK();
    }
    const size_t target_bytes = count * sizeof(uint32_t);
    writer::MemoryReporter::Reservation replacement;
    RETURN_IF_ERROR(docids_reservation.prepare_replacement(target_bytes, &replacement));
    docids.reserve(count);
    DCHECK_EQ(docids.capacity(), count);
    docids_reservation = std::move(replacement);
    return Status::OK();
}

Status SniiPostingReadContext::DecoderWorkspace::reserve_csr(std::vector<uint32_t>* pos_flat,
                                                             size_t position_count,
                                                             std::vector<uint32_t>* pos_off,
                                                             size_t offset_count) {
    DCHECK(reservations_enabled);
    DCHECK_EQ(pos_flat, &positions_flat);
    DCHECK_EQ(pos_off, &position_offsets);
    if (position_count > std::numeric_limits<size_t>::max() / sizeof(uint32_t) ||
        offset_count > std::numeric_limits<size_t>::max() / sizeof(uint32_t)) {
        return Status::Error<ErrorCode::MEM_LIMIT_EXCEEDED, false>(
                "posting cursor: position workspace size overflows size_t");
    }

    const bool grow_positions = positions_flat.capacity() < position_count;
    const bool grow_offsets = position_offsets.capacity() < offset_count;
    if (!grow_positions && !grow_offsets) {
        DCHECK_EQ(positions_reservation.bytes(), positions_flat.capacity() * sizeof(uint32_t));
        DCHECK_EQ(position_offsets_reservation.bytes(),
                  position_offsets.capacity() * sizeof(uint32_t));
        return Status::OK();
    }

    writer::MemoryReporter::Reservation replacement_positions;
    writer::MemoryReporter::Reservation replacement_offsets;
    if (grow_positions) {
        RETURN_IF_ERROR(positions_reservation.prepare_replacement(position_count * sizeof(uint32_t),
                                                                  &replacement_positions));
    }
    if (grow_offsets) {
        RETURN_IF_ERROR(position_offsets_reservation.prepare_replacement(
                offset_count * sizeof(uint32_t), &replacement_offsets));
    }

    {
        std::vector<uint32_t> new_positions;
        std::vector<uint32_t> new_offsets;
        if (grow_positions) {
            new_positions.reserve(position_count);
            DCHECK_EQ(new_positions.capacity(), position_count);
            positions_flat.swap(new_positions);
        }
        if (grow_offsets) {
            new_offsets.reserve(offset_count);
            DCHECK_EQ(new_offsets.capacity(), offset_count);
            position_offsets.swap(new_offsets);
        }
    }
    if (grow_positions) {
        positions_reservation = std::move(replacement_positions);
    }
    if (grow_offsets) {
        position_offsets_reservation = std::move(replacement_offsets);
    }
    return Status::OK();
}

Status SniiPostingReadContext::DecoderWorkspace::reserve_decompression(
        size_t bytes, std::vector<uint8_t>** buffer) {
    DCHECK(reservations_enabled);
    DCHECK(buffer != nullptr);
    if (decompressed.capacity() < bytes) {
        writer::MemoryReporter::Reservation replacement;
        RETURN_IF_ERROR(decompressed_reservation.prepare_replacement(bytes, &replacement));
        {
            std::vector<uint8_t> new_decompressed;
            new_decompressed.reserve(bytes);
            DCHECK_EQ(new_decompressed.capacity(), bytes);
            decompressed.swap(new_decompressed);
        }
        decompressed_reservation = std::move(replacement);
    } else {
        DCHECK_EQ(decompressed_reservation.bytes(), decompressed.capacity());
    }
    *buffer = &decompressed;
    return Status::OK();
}

void SniiPostingReadContext::DecoderWorkspace::release_large_buffers(
        size_t retained_capacity_limit_bytes) {
    prelude = format::FrqPreludeReader();
    if (capacity_bytes() <= retained_capacity_limit_bytes) {
        return;
    }
    std::vector<uint8_t>().swap(docs_scratch);
    std::vector<uint8_t>().swap(prx_scratch);
    std::vector<uint8_t>().swap(decompressed);
    std::vector<uint32_t>().swap(docids);
    std::vector<uint32_t>().swap(positions_flat);
    std::vector<uint32_t>().swap(position_offsets);
    std::vector<DestinationPostingRun>().swap(destination_runs);
    std::vector<uint32_t>().swap(frequencies);
    docs_scratch_reservation.reset();
    prx_scratch_reservation.reset();
    docids_reservation.reset();
    positions_reservation.reset();
    position_offsets_reservation.reset();
    destination_runs_reservation.reset();
    frequencies_reservation.reset();
    decompressed_reservation.reset();
    DCHECK_LE(capacity_bytes(), retained_capacity_limit_bytes);
}

Status SniiPostingReadContext::poison(Status status) {
    DCHECK(!status.ok());
    if (failed_.ok()) {
        failed_ = std::move(status);
    }
    return failed_;
}

Status SniiPostingReadContext::init() {
    if (initialized_) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "posting_read_context: init called twice");
    }
    if (index_ == nullptr || index_->reader() == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "posting_read_context: null source index");
    }
    if (total_read_ahead_budget_bytes_ < 2 ||
        total_read_ahead_budget_bytes_ > kMaxReadAheadBudgetBytes) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "posting_read_context: total read-ahead budget outside [2, {}]",
                kMaxReadAheadBudgetBytes);
    }

    posting_region_ = index_->section_refs().posting_region;
    RETURN_IF_ERROR(validate_posting_region(posting_region_, index_->reader()->size()));
    decoder_workspace_.init_memory_reporter(memory_reporter_);
    posting_cache_ = std::make_unique<SharedAlignedRegionCache>(
            index_->reader(), posting_region_.offset, posting_region_.length,
            total_read_ahead_budget_bytes_, memory_reporter_);
    RETURN_IF_ERROR(posting_cache_->init());
    initialized_ = true;
    return Status::OK();
}

Status SniiPostingReadContext::validate_next_range(const format::RegionRef& range,
                                                   bool has_previous, uint64_t previous_end,
                                                   const char* stream, uint64_t* end) const {
    DCHECK(end != nullptr);
    if (range.length == 0 || range.offset < posting_region_.offset) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "posting_read_context: invalid {} term range", stream);
    }
    const uint64_t relative_offset = range.offset - posting_region_.offset;
    if (relative_offset > posting_region_.length ||
        range.length > posting_region_.length - relative_offset) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "posting_read_context: {} term range outside posting region", stream);
    }
    if (has_previous && range.offset < previous_end) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "posting_read_context: {} term ranges are not monotone", stream);
    }
    *end = range.offset + range.length;
    return Status::OK();
}

Status SniiPostingReadContext::acquire_term(bool has_docs_range, bool has_prx_range,
                                            const format::RegionRef& docs_range,
                                            const format::RegionRef& prx_range,
                                            std::unique_ptr<TermLease>* lease) {
    DCHECK(lease != nullptr);
    DCHECK(*lease == nullptr);
    if (!initialized_) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "posting_read_context: acquire before init");
    }
    if (!failed_.ok()) {
        return failed_;
    }
    if (term_active_) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "posting_read_context: concurrent term cursor");
    }

    uint64_t docs_end = 0;
    uint64_t prx_end = 0;
    if (has_docs_range) {
        Status status =
                validate_next_range(docs_range, has_docs_range_, last_docs_end_, "docs", &docs_end);
        if (!status.ok()) {
            return poison(status);
        }
        last_docs_end_ = docs_end;
        has_docs_range_ = true;
    }
    if (has_prx_range) {
        Status status =
                validate_next_range(prx_range, has_prx_range_, last_prx_end_, "prx", &prx_end);
        if (!status.ok()) {
            return poison(status);
        }
        last_prx_end_ = prx_end;
        has_prx_range_ = true;
    }

    term_active_ = true;
    lease->reset(new TermLease(this));
    return Status::OK();
}

void SniiPostingReadContext::release_term() {
    DCHECK(term_active_);
    term_active_ = false;
    decoder_workspace_.release_large_buffers(retained_decoder_workspace_limit_bytes());
}

Status SniiPostingReadContext::poison_active_term(Status status, TermLease* lease) {
    DCHECK(lease != nullptr);
    DCHECK_EQ(lease->context_, this);
    DCHECK(term_active_);
    Status first = poison(std::move(status));
    lease->context_ = nullptr;
    release_term();
    return first;
}

uint64_t SniiPostingReadContext::docs_read_calls() const {
    DCHECK(initialized_);
    return posting_cache_->read_calls(PostingStream::kDocs);
}

uint64_t SniiPostingReadContext::prx_read_calls() const {
    DCHECK(initialized_);
    return posting_cache_->read_calls(PostingStream::kPrx);
}

uint64_t SniiPostingReadContext::docs_buffer_hits() const {
    DCHECK(initialized_);
    return posting_cache_->buffer_hits(PostingStream::kDocs);
}

uint64_t SniiPostingReadContext::prx_buffer_hits() const {
    DCHECK(initialized_);
    return posting_cache_->buffer_hits(PostingStream::kPrx);
}

uint64_t SniiPostingReadContext::physical_read_ranges() const {
    DCHECK(initialized_);
    return posting_cache_->physical_read_ranges();
}

uint64_t SniiPostingReadContext::physical_read_bytes() const {
    DCHECK(initialized_);
    return posting_cache_->physical_read_bytes();
}

size_t SniiPostingReadContext::resident_read_ahead_capacity_bytes() const {
    DCHECK(initialized_);
    return posting_cache_->resident_capacity_bytes();
}

size_t SniiPostingReadContext::decoder_workspace_capacity_bytes() const {
    DCHECK(initialized_);
    return decoder_workspace_.capacity_bytes();
}

Status SniiPostingCursor::poison(Status status) {
    DCHECK(!status.ok());
    if (failed_.ok()) {
        if (term_lease_ != nullptr) {
            failed_ = read_context_->poison_active_term(std::move(status), term_lease_.get());
            term_lease_.reset();
        } else {
            failed_ = std::move(status);
        }
    }
    return failed_;
}

Status SniiPostingCursor::validate_entry_geometry() {
    if (entry_.df == 0) {
        return posting_corruption("zero-df dictionary entry", source_ordinal_);
    }
    if (entry_.kind == format::DictEntryKind::kInline) {
        if (entry_.enc != format::DictEntryEnc::kSlim) {
            return posting_corruption("inline entry is not slim", source_ordinal_);
        }
        if (entry_.inline_dd_disk_len != entry_.dd_meta.disk_len ||
            entry_.inline_dd_disk_len > entry_.frq_bytes.size()) {
            return posting_corruption("inline dd geometry mismatch", source_ordinal_);
        }
        const uint64_t freq_len = entry_.frq_bytes.size() - entry_.inline_dd_disk_len;
        if (entry_.freq_meta.disk_len != freq_len) {
            return posting_corruption("inline freq geometry mismatch", source_ordinal_);
        }
        shape_ = Shape::kFlat;
        return Status::OK();
    }

    if (entry_.kind != format::DictEntryKind::kPodRef) {
        return posting_corruption("unknown dictionary entry kind", source_ordinal_);
    }
    if (entry_.enc == format::DictEntryEnc::kWindowed) {
        if (entry_.prelude_len == 0 || entry_.prelude_len > entry_.frq_docs_len ||
            entry_.frq_docs_len > entry_.frq_len) {
            return posting_corruption("invalid windowed frq geometry", source_ordinal_);
        }
        shape_ = Shape::kWindowed;
        return Status::OK();
    }
    if (entry_.enc != format::DictEntryEnc::kSlim) {
        return posting_corruption("unknown dictionary entry encoding", source_ordinal_);
    }
    if (entry_.prelude_len != 0 || entry_.frq_docs_len != entry_.dd_meta.disk_len ||
        entry_.frq_docs_len > entry_.frq_len) {
        return posting_corruption("invalid slim frq geometry", source_ordinal_);
    }
    if (entry_.freq_meta.disk_len != entry_.frq_len - entry_.frq_docs_len) {
        return posting_corruption("slim freq geometry mismatch", source_ordinal_);
    }
    shape_ = Shape::kFlat;
    return Status::OK();
}

Status SniiPostingCursor::prepare_flat_ranges() {
    if (entry_.kind == format::DictEntryKind::kInline) {
        flat_dd_len_ = entry_.inline_dd_disk_len;
        flat_prx_len_ = entry_.prx_bytes.size();
        return Status::OK();
    }

    uint64_t frq_len = 0;
    RETURN_IF_ERROR(index_->resolve_frq_window(entry_, frq_base_, &flat_dd_abs_, &frq_len));
    if (frq_len != entry_.frq_len) {
        return posting_corruption("resolved slim frq length mismatch", source_ordinal_);
    }
    flat_dd_len_ = entry_.frq_docs_len;
    if (term_has_positions_) {
        RETURN_IF_ERROR(
                index_->resolve_prx_window(entry_, prx_base_, &flat_prx_abs_, &flat_prx_len_));
        if (flat_prx_len_ != entry_.prx_len) {
            return posting_corruption("resolved slim prx length mismatch", source_ordinal_);
        }
    }
    return Status::OK();
}

Status SniiPostingCursor::prepare_windowed_ranges() {
    RETURN_IF_ERROR(index_->resolve_frq_window(entry_, frq_base_, &flat_dd_abs_, &flat_dd_len_));
    if (flat_dd_len_ != entry_.frq_len - entry_.prelude_len || flat_dd_abs_ < entry_.prelude_len) {
        return posting_corruption("resolved windowed frq geometry mismatch", source_ordinal_);
    }
    if (term_has_positions_) {
        RETURN_IF_ERROR(
                index_->resolve_prx_window(entry_, prx_base_, &flat_prx_abs_, &flat_prx_len_));
        if (flat_prx_len_ != entry_.prx_len) {
            return posting_corruption("resolved windowed prx length mismatch", source_ordinal_);
        }
    }
    return Status::OK();
}

Status SniiPostingCursor::prepare_windowed() {
    DCHECK(workspace_ != nullptr);
    Slice prelude_bytes;
    RETURN_IF_ERROR(read_context_->posting_cache_->resolve(
            PostingStream::kDocs, flat_dd_abs_ - entry_.prelude_len, entry_.prelude_len,
            &workspace_->docs_scratch, &prelude_bytes,
            read_context_->memory_reporter_ == nullptr ? nullptr
                                                       : &workspace_->docs_scratch_reservation));
    RETURN_IF_ERROR(format::FrqPreludeReader::open(prelude_bytes, &workspace_->prelude));
    if (workspace_->prelude.has_prx() != term_has_positions_) {
        return posting_corruption("windowed prelude position shape differs from entry",
                                  source_ordinal_);
    }

    uint64_t docs_prefix_len = 0;
    RETURN_IF_ERROR(checked_add(entry_.prelude_len, workspace_->prelude.dd_block_len(),
                                "posting_cursor: windowed docs prefix overflow", &docs_prefix_len));
    if (docs_prefix_len != entry_.frq_docs_len) {
        return posting_corruption("windowed docs prefix mismatch", source_ordinal_);
    }
    uint64_t encoded_frq_len = 0;
    RETURN_IF_ERROR(checked_add(docs_prefix_len, workspace_->prelude.freq_block_len(),
                                "posting_cursor: windowed frq length overflow", &encoded_frq_len));
    if (encoded_frq_len != entry_.frq_len) {
        return posting_corruption("windowed frq blocks do not tile entry", source_ordinal_);
    }

    uint64_t dd_bytes = 0;
    uint64_t freq_bytes = 0;
    uint64_t prx_bytes = 0;
    uint64_t docs = 0;
    uint32_t previous_last_docid = 0;
    bool has_previous_window = false;
    for (uint32_t window = 0; window < workspace_->prelude.n_windows(); ++window) {
        format::WindowMeta meta;
        RETURN_IF_ERROR(workspace_->prelude.window(window, &meta));
        if (meta.doc_count == 0 || meta.dd_off != dd_bytes || meta.prx_off != prx_bytes ||
            (workspace_->prelude.has_freq() && meta.freq_off != freq_bytes)) {
            return posting_corruption("non-contiguous window metadata", source_ordinal_);
        }
        if ((!has_previous_window && meta.win_base != 0) ||
            (has_previous_window &&
             (meta.win_base != previous_last_docid || meta.last_docid <= previous_last_docid))) {
            return posting_corruption("invalid window docid chain", source_ordinal_);
        }
        if (meta.last_docid >= index_->stats().doc_count) {
            return posting_corruption("window last docid outside index", source_ordinal_);
        }
        RETURN_IF_ERROR(checked_add(dd_bytes, meta.dd_disk_len,
                                    "posting_cursor: window dd bytes overflow", &dd_bytes));
        RETURN_IF_ERROR(checked_add(freq_bytes, meta.freq_disk_len,
                                    "posting_cursor: window freq bytes overflow", &freq_bytes));
        RETURN_IF_ERROR(checked_add(prx_bytes, meta.prx_len,
                                    "posting_cursor: window prx bytes overflow", &prx_bytes));
        RETURN_IF_ERROR(checked_add(docs, meta.doc_count,
                                    "posting_cursor: window doc count overflow", &docs));
        previous_last_docid = meta.last_docid;
        has_previous_window = true;
    }
    if (dd_bytes != workspace_->prelude.dd_block_len() ||
        freq_bytes != workspace_->prelude.freq_block_len() || prx_bytes != entry_.prx_len ||
        docs != entry_.df) {
        return posting_corruption("window directory totals mismatch", source_ordinal_);
    }
    return Status::OK();
}

Status SniiPostingCursor::init() {
    if (initialized_) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "posting_cursor: init called twice");
    }
    if (read_context_ == nullptr || index_ == nullptr || rowid_conversion_ == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "posting_cursor: null read context or rowid conversion");
    }
    if (!read_context_->initialized()) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "posting_cursor: source read context not initialized");
    }
    if (!read_context_->failed_status().ok()) {
        return read_context_->failed_status();
    }
    if (index_->tier() == format::IndexTier::kT1) {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED, false>(
                "posting_cursor: positions index is required");
    }
    if (!index_->has_positions()) {
        return posting_corruption("positions tier lacks positions capability", source_ordinal_);
    }
    if (source_ordinal_ >= rowid_conversion_->source_segment_count()) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "posting_cursor: source ordinal outside rowid conversion");
    }
    if (index_->stats().doc_count !=
        rowid_conversion_->source_segment_doc_counts()[source_ordinal_]) {
        return posting_corruption("rowid conversion size differs from source doc count",
                                  source_ordinal_);
    }
    source_mapping_ = rowid_conversion_->source_mapping(source_ordinal_);
    source_has_deletions_ = rowid_conversion_->source_has_deletions(source_ordinal_);

    RETURN_IF_ERROR(validate_entry_geometry());
    const bool has_pod_ranges = entry_.kind == format::DictEntryKind::kPodRef;
    format::RegionRef docs_range;
    format::RegionRef prx_range;
    if (shape_ == Shape::kWindowed) {
        RETURN_IF_ERROR(prepare_windowed_ranges());
        docs_range.offset = flat_dd_abs_ - entry_.prelude_len;
        docs_range.length = entry_.frq_docs_len;
        prx_range.offset = flat_prx_abs_;
        prx_range.length = flat_prx_len_;
    } else {
        RETURN_IF_ERROR(prepare_flat_ranges());
        if (has_pod_ranges) {
            docs_range.offset = flat_dd_abs_;
            docs_range.length = flat_dd_len_;
            prx_range.offset = flat_prx_abs_;
            prx_range.length = flat_prx_len_;
        }
    }
    RETURN_IF_ERROR(read_context_->acquire_term(has_pod_ranges,
                                                has_pod_ranges && term_has_positions_, docs_range,
                                                prx_range, &term_lease_));
    workspace_ = &read_context_->decoder_workspace_;
    workspace_->destination_runs.clear();
    workspace_->frequencies.clear();
    next_destination_run_ = 0;
    if (shape_ == Shape::kWindowed) {
        const Status status = prepare_windowed();
        if (!status.ok()) {
            return poison(status);
        }
    }
    initialized_ = true;
    return Status::OK();
}

Status SniiPostingCursor::decode_prx(Slice bytes, format::PrxDecodedShape* shape) {
    DCHECK(workspace_ != nullptr);
    DCHECK(shape != nullptr);
    ByteSource source(bytes);
    format::PrxDecodeContext decode_context {
            .shape = shape,
            .allocation_gate = workspace_->reservations_enabled ? workspace_ : nullptr};
    RETURN_IF_ERROR(format::read_prx_window_csr(&source, &workspace_->positions_flat,
                                                &workspace_->position_offsets, &decode_context));
    if (!source.eof()) {
        return posting_corruption("trailing bytes after prx frame", source_ordinal_);
    }
    return Status::OK();
}

Status SniiPostingCursor::decode_dd(Slice bytes, const format::FrqRegionMeta& meta,
                                    uint64_t win_base, uint32_t expected_doc_count) {
    DCHECK(workspace_ != nullptr);
    if (!workspace_->reservations_enabled) {
        return format::decode_dd_region(bytes, meta, win_base, expected_doc_count,
                                        &workspace_->docids);
    }
    return format::decode_dd_region(bytes, meta, win_base, expected_doc_count, workspace_,
                                    &workspace_->docids);
}

Status SniiPostingCursor::load_flat_chunk() {
    DCHECK(workspace_ != nullptr);
    Slice dd_bytes;
    Slice prx_bytes;
    if (entry_.kind == format::DictEntryKind::kInline) {
        dd_bytes = Slice(entry_.frq_bytes.data(), static_cast<size_t>(flat_dd_len_));
        prx_bytes = Slice(entry_.prx_bytes);
    } else {
        RETURN_IF_ERROR(read_context_->posting_cache_->resolve(
                PostingStream::kDocs, flat_dd_abs_, flat_dd_len_, &workspace_->docs_scratch,
                &dd_bytes,
                read_context_->memory_reporter_ == nullptr
                        ? nullptr
                        : &workspace_->docs_scratch_reservation));
        if (term_has_positions_) {
            RETURN_IF_ERROR(read_context_->posting_cache_->resolve(
                    PostingStream::kPrx, flat_prx_abs_, flat_prx_len_, &workspace_->prx_scratch,
                    &prx_bytes,
                    read_context_->memory_reporter_ == nullptr
                            ? nullptr
                            : &workspace_->prx_scratch_reservation));
        }
    }

    RETURN_IF_ERROR(workspace_->reserve_docids(entry_.df));
    RETURN_IF_ERROR(decode_dd(dd_bytes, entry_.dd_meta, /*win_base=*/0, entry_.df));
    format::PrxDecodedShape prx_shape;
    if (term_has_positions_) {
        RETURN_IF_ERROR(decode_prx(prx_bytes, &prx_shape));
    }
    if (workspace_->docids.size() != entry_.df || workspace_->docids.empty() ||
        workspace_->docids.back() >= index_->stats().doc_count) {
        return posting_corruption("decoded docids differ from flat entry shape", source_ordinal_);
    }
    if (term_has_positions_ &&
        (prx_shape.total_docs != entry_.df ||
         prx_shape.total_positions != workspace_->positions_flat.size() ||
         prx_shape.has_zero_frequency ||
         workspace_->position_offsets.size() != static_cast<size_t>(entry_.df) + 1 ||
         workspace_->position_offsets.empty() || workspace_->position_offsets.front() != 0 ||
         workspace_->position_offsets.back() != workspace_->positions_flat.size())) {
        return posting_corruption("dd/prx document shape mismatch", source_ordinal_);
    }
    decoded_docs_ = entry_.df;
    if (term_has_positions_) {
        decoded_total_freq_ = prx_shape.total_positions;
        decoded_max_freq_ = prx_shape.max_frequency;
    }
    flat_loaded_ = true;
    return Status::OK();
}

Status SniiPostingCursor::load_windowed_chunk() {
    DCHECK(workspace_ != nullptr);
    format::WindowMeta meta;
    RETURN_IF_ERROR(workspace_->prelude.window(next_window_, &meta));
    reader::WindowAbsRange range;
    RETURN_IF_ERROR(reader::windowed_window_range(
            *index_, entry_, frq_base_, prx_base_, workspace_->prelude, next_window_,
            /*want_positions=*/term_has_positions_, /*want_freq=*/false, &range));

    Slice dd_bytes;
    Slice prx_bytes;
    RETURN_IF_ERROR(read_context_->posting_cache_->resolve(
            PostingStream::kDocs, range.dd_off, range.dd_len, &workspace_->docs_scratch, &dd_bytes,
            read_context_->memory_reporter_ == nullptr ? nullptr
                                                       : &workspace_->docs_scratch_reservation));
    if (term_has_positions_) {
        RETURN_IF_ERROR(read_context_->posting_cache_->resolve(
                PostingStream::kPrx, range.prx_off, range.prx_len, &workspace_->prx_scratch,
                &prx_bytes,
                read_context_->memory_reporter_ == nullptr ? nullptr
                                                           : &workspace_->prx_scratch_reservation));
    }
    RETURN_IF_ERROR(workspace_->reserve_docids(meta.doc_count));
    RETURN_IF_ERROR(decode_dd(dd_bytes,
                              format::FrqRegionMeta {.zstd = meta.dd_zstd,
                                                     .uncomp_len = meta.dd_uncomp_len,
                                                     .disk_len = meta.dd_disk_len,
                                                     .crc = meta.crc_dd,
                                                     .verify_crc = meta.verify_crc},
                              meta.win_base, meta.doc_count));
    format::PrxDecodedShape prx_shape;
    if (term_has_positions_) {
        RETURN_IF_ERROR(decode_prx(prx_bytes, &prx_shape));
    }
    if (workspace_->docids.size() != meta.doc_count || workspace_->docids.empty() ||
        workspace_->docids.back() != meta.last_docid ||
        (next_window_ != 0 && workspace_->docids.front() <= meta.win_base)) {
        return posting_corruption("window docid shape or last docid mismatch", source_ordinal_);
    }
    if (term_has_positions_ &&
        (prx_shape.total_docs != meta.doc_count ||
         prx_shape.total_positions != workspace_->positions_flat.size() ||
         prx_shape.has_zero_frequency ||
         workspace_->position_offsets.size() != static_cast<size_t>(meta.doc_count) + 1 ||
         workspace_->position_offsets.empty() || workspace_->position_offsets.front() != 0 ||
         workspace_->position_offsets.back() != workspace_->positions_flat.size())) {
        return posting_corruption("dd/prx document shape mismatch", source_ordinal_);
    }
    if (term_has_positions_ && entry_.term_stats_present &&
        prx_shape.max_frequency != meta.max_freq) {
        return posting_corruption("window max frequency mismatch", source_ordinal_);
    }
    if (decoded_docs_ > entry_.df || meta.doc_count > entry_.df - decoded_docs_) {
        return posting_corruption("decoded document count exceeds df", source_ordinal_);
    }
    decoded_docs_ += meta.doc_count;
    if (term_has_positions_) {
        if (prx_shape.total_positions >
            std::numeric_limits<uint64_t>::max() - decoded_total_freq_) {
            return posting_corruption("total term frequency overflow", source_ordinal_);
        }
        decoded_total_freq_ += prx_shape.total_positions;
        decoded_max_freq_ = std::max(decoded_max_freq_, prx_shape.max_frequency);
    }
    ++next_window_;
    return Status::OK();
}

Status SniiPostingCursor::load_next_chunk(bool* loaded) {
    DCHECK(loaded != nullptr);
    DCHECK(workspace_ != nullptr);
    *loaded = false;
    // Both decode paths resize and validate these buffers. Clearing them first would force a cold
    // grow and zero-fill on every warm re-decode. Stale contents are never read when not loaded.

    if (shape_ == Shape::kFlat) {
        if (flat_loaded_) {
            return Status::OK();
        }
        RETURN_IF_ERROR(load_flat_chunk());
        *loaded = true;
        return Status::OK();
    }
    if (next_window_ >= workspace_->prelude.n_windows()) {
        return Status::OK();
    }
    RETURN_IF_ERROR(load_windowed_chunk());
    *loaded = true;
    return Status::OK();
}

Status SniiPostingCursor::finish_source() {
    if (decoded_docs_ != entry_.df) {
        return posting_corruption("decoded document count differs from df", source_ordinal_);
    }
    if (entry_.term_stats_present &&
        (decoded_total_freq_ != entry_.ttf_delta || decoded_max_freq_ != entry_.max_freq)) {
        return posting_corruption("decoded term statistics mismatch", source_ordinal_);
    }
    exhausted_ = true;
    term_lease_.reset();
    return Status::OK();
}

Status SniiPostingCursor::map_decoded_chunk() {
    DCHECK(workspace_ != nullptr);
    std::vector<DestinationPostingRun>& runs = workspace_->destination_runs;
    std::vector<uint32_t>& docids = workspace_->docids;
    std::vector<uint32_t>& frequencies = workspace_->frequencies;
    runs.clear();
    frequencies.clear();
    const size_t document_count = docids.size();
    const size_t max_run_count =
            std::min(document_count, rowid_conversion_->destination_segment_doc_counts().size());
    RETURN_IF_ERROR(
            workspace_->reserve_remapped(document_count, max_run_count, term_has_positions_));

    auto append_live_document = [&](uint32_t segment, uint32_t docid, size_t live_docs) {
        if (runs.empty() || runs.back().destination_segment != segment) {
            if (!runs.empty()) {
                runs.back().document_end = static_cast<uint32_t>(live_docs);
            }
            runs.push_back({.destination_segment = segment});
        }
        docids[live_docs] = docid;
    };

    if (!source_has_deletions_) {
        if (term_has_positions_) {
            // Frequencies are adjacent position-offset deltas; fill them in
            // their own pass so the remap loop below stays a pure gather.
            const uint32_t* offsets = workspace_->position_offsets.data();
            for (size_t ordinal = 0; ordinal < document_count; ++ordinal) {
                frequencies.push_back(offsets[ordinal + 1] - offsets[ordinal]);
            }
        }
        // The gather walks source_mapping_ at monotonically increasing but
        // sparse indexes the hardware prefetcher cannot follow; the future
        // lookup indexes are already decoded, so prefetch them explicitly.
        // Without deletions every mapping entry is live, so the destination
        // segment can never equal the uint32 sentinel.
        constexpr size_t kMapPrefetchDistance = 16;
        uint32_t current_segment = std::numeric_limits<uint32_t>::max();
        for (size_t ordinal = 0; ordinal < document_count; ++ordinal) {
            if (ordinal + kMapPrefetchDistance < document_count) {
                __builtin_prefetch(&source_mapping_[docids[ordinal + kMapPrefetchDistance]]);
            }
            const auto [segment, docid] = source_mapping_[docids[ordinal]];
            if (segment != current_segment) {
                if (!runs.empty()) {
                    runs.back().document_end = static_cast<uint32_t>(ordinal);
                }
                runs.push_back({.destination_segment = segment});
                current_segment = segment;
            }
            docids[ordinal] = docid;
        }
    } else if (!term_has_positions_) {
        constexpr uint32_t kDeleted = std::numeric_limits<uint32_t>::max();
        size_t live_docs = 0;
        for (uint32_t source_docid : docids) {
            const auto [segment, docid] = source_mapping_[source_docid];
            if (segment != kDeleted) {
                append_live_document(segment, docid, live_docs++);
            }
        }
        docids.resize(live_docs);
    } else {
        constexpr uint32_t kDeleted = std::numeric_limits<uint32_t>::max();
        size_t write_position = 0;
        size_t live_docs = 0;
        workspace_->position_offsets[0] = 0;
        for (size_t ordinal = 0; ordinal < docids.size(); ++ordinal) {
            const auto [segment, docid] = source_mapping_[docids[ordinal]];
            if (segment == kDeleted) {
                continue;
            }
            const size_t begin = workspace_->position_offsets[ordinal];
            const size_t end = workspace_->position_offsets[ordinal + 1];
            const uint32_t frequency = static_cast<uint32_t>(end - begin);
            if (write_position != begin) {
                std::copy(workspace_->positions_flat.begin() + begin,
                          workspace_->positions_flat.begin() + end,
                          workspace_->positions_flat.begin() + write_position);
            }
            write_position += frequency;
            append_live_document(segment, docid, live_docs);
            frequencies.push_back(frequency);
            workspace_->position_offsets[++live_docs] = static_cast<uint32_t>(write_position);
        }
        docids.resize(live_docs);
        workspace_->positions_flat.resize(write_position);
        workspace_->position_offsets.resize(live_docs + 1);
    }

    if (!runs.empty()) {
        runs.back().document_end = static_cast<uint32_t>(docids.size());
    }
    next_destination_run_ = 0;
    return Status::OK();
}

void SniiPostingCursor::emit_next_mapped_run(RemappedPostingChunk* chunk) {
    DCHECK(chunk != nullptr);
    DCHECK(workspace_ != nullptr);
    DCHECK_LT(next_destination_run_, workspace_->destination_runs.size());

    const size_t run_ordinal = next_destination_run_++;
    const DestinationPostingRun& run = workspace_->destination_runs[run_ordinal];
    const size_t document_begin =
            run_ordinal == 0 ? 0 : workspace_->destination_runs[run_ordinal - 1].document_end;
    const size_t document_end = run.document_end;
    DCHECK_LT(document_begin, document_end);
    DCHECK_LE(document_end, workspace_->docids.size());
    const size_t document_count = document_end - document_begin;

    chunk->destination_segment = run.destination_segment;
    chunk->destination_docids =
            std::span<const uint32_t>(workspace_->docids).subspan(document_begin, document_count);
    if (!term_has_positions_) {
        return;
    }

    chunk->freqs = std::span<const uint32_t>(workspace_->frequencies)
                           .subspan(document_begin, document_count);
    chunk->position_offsets = std::span<const uint32_t>(workspace_->position_offsets)
                                      .subspan(document_begin, document_count + 1);
    const size_t position_begin = chunk->position_offsets.front();
    const size_t position_end = chunk->position_offsets.back();
    DCHECK_LE(position_begin, position_end);
    DCHECK_LE(position_end, workspace_->positions_flat.size());
    chunk->positions_flat = std::span<const uint32_t>(workspace_->positions_flat)
                                    .subspan(position_begin, position_end - position_begin);
}

Status SniiPostingCursor::next_chunk(RemappedPostingChunk* chunk, bool* has_chunk) {
    if (chunk == nullptr || has_chunk == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "posting_cursor: null chunk output");
    }
    *chunk = {};
    *has_chunk = false;
    if (!failed_.ok()) {
        return failed_;
    }
    if (!initialized_) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "posting_cursor: next_chunk before init");
    }
    if (exhausted_) {
        return Status::OK();
    }

    if (next_destination_run_ < workspace_->destination_runs.size()) {
        emit_next_mapped_run(chunk);
        *has_chunk = true;
        return Status::OK();
    }

    while (true) {
        bool loaded = false;
        const Status status = load_next_chunk(&loaded);
        if (!status.ok()) {
            return poison(status);
        }
        if (!loaded) {
            *chunk = {};
            const Status finish_status = finish_source();
            if (!finish_status.ok()) {
                return poison(finish_status);
            }
            return Status::OK();
        }
        const Status map_status = map_decoded_chunk();
        if (!map_status.ok()) {
            return poison(map_status);
        }
        if (!workspace_->destination_runs.empty()) {
            emit_next_mapped_run(chunk);
            *has_chunk = true;
            return Status::OK();
        }
        *chunk = {};
    }
}

} // namespace doris::snii::compaction
