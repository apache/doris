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

#include <algorithm>
#include <cstdint>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/writer/memory_reporter.h"

namespace doris::snii::writer {

struct MutableTermPostingSpan {
    std::span<uint32_t> docids;
    std::span<uint32_t> freqs;
    std::span<uint32_t> positions_flat;
};

// Reusable, reservation-backed transfer storage for one source fill. A source
// may append multiple runs during one fill, but every run must agree on whether
// transient frequency statistics are present. clear_reuse() preserves capacity
// and its memory charge.
class TermPostingBuffer {
public:
    explicit TermPostingBuffer(MemoryReporter* memory_reporter)
            : memory_reporter_(memory_reporter),
              capacity_reservation_(memory_reporter == nullptr
                                            ? MemoryReporter::Reservation()
                                            : memory_reporter->make_reservation()) {}

    TermPostingBuffer(const TermPostingBuffer&) = delete;
    TermPostingBuffer& operator=(const TermPostingBuffer&) = delete;
    TermPostingBuffer(TermPostingBuffer&&) = delete;
    TermPostingBuffer& operator=(TermPostingBuffer&&) = delete;

    size_t document_count() const { return docids_.size(); }
    bool empty() const { return docids_.empty(); }

    void clear_reuse() {
        docids_.clear();
        freqs_.clear();
        positions_flat_.clear();
        has_freqs_.reset();
    }

    void clear_reuse_and_release_excess(size_t max_retained_capacity) {
        clear_reuse();
        bool released = false;
        if (docids_.capacity() > max_retained_capacity) {
            std::vector<uint32_t>().swap(docids_);
            released = true;
        }
        if (freqs_.capacity() > max_retained_capacity) {
            std::vector<uint32_t>().swap(freqs_);
            released = true;
        }
        if (positions_flat_.capacity() > max_retained_capacity) {
            std::vector<uint32_t>().swap(positions_flat_);
            released = true;
        }
        if (released && memory_reporter_ != nullptr) {
            uint64_t retained_bytes = 0;
            DORIS_CHECK(capacity_bytes(docids_.capacity(), freqs_.capacity(),
                                       positions_flat_.capacity(), &retained_bytes)
                                .ok());
            DORIS_CHECK(capacity_reservation_.set_bytes(retained_bytes).ok());
        }
    }

    Status append(std::span<const uint32_t> docids, std::span<const uint32_t> freqs,
                  std::span<const uint32_t> positions_flat) {
        const bool has_freqs = !freqs.empty();
        if (has_freqs && freqs.size() != docids.size()) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "term posting buffer: freqs length must equal docids");
        }
        if (!has_freqs && !positions_flat.empty()) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "term posting buffer: positions require parallel freqs");
        }
        uint64_t expected_positions = 0;
        for (uint32_t freq : freqs) {
            if (freq > std::numeric_limits<uint64_t>::max() - expected_positions) {
                return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                        "term posting buffer: position count overflow");
            }
            expected_positions += freq;
        }
        if (!positions_flat.empty() && expected_positions != positions_flat.size()) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "term posting buffer: positions count must equal sum(freqs)");
        }
        if (has_freqs_.has_value() && *has_freqs_ != has_freqs) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "term posting buffer: frequency shape changed within one fill");
        }

        MutableTermPostingSpan destination;
        RETURN_IF_ERROR(
                grow_uninitialized(docids.size(), has_freqs, positions_flat.size(), &destination));
        std::ranges::copy(docids, destination.docids.begin());
        std::ranges::copy(freqs, destination.freqs.begin());
        std::ranges::copy(positions_flat, destination.positions_flat.begin());
        return Status::OK();
    }

    // Extends the current fill once and exposes the new tail for direct decode.
    // The caller must initialize every returned element before returning from
    // TermPostingSource::fill. Shape and capacity changes are committed only
    // after all reservations succeed.
    Status grow_uninitialized(size_t document_count, bool has_freqs, size_t position_count,
                              MutableTermPostingSpan* destination) {
        if (destination == nullptr) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "term posting buffer: null writable span destination");
        }
        if (!has_freqs && position_count != 0) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "term posting buffer: positions require parallel freqs");
        }
        if (has_freqs_.has_value() && *has_freqs_ != has_freqs) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "term posting buffer: frequency shape changed within one fill");
        }

        const size_t doc_begin = docids_.size();
        const size_t freq_begin = freqs_.size();
        const size_t position_begin = positions_flat_.size();
        size_t target_docids = 0;
        size_t target_freqs = 0;
        size_t target_positions = 0;
        RETURN_IF_ERROR(checked_size(doc_begin, document_count, &target_docids));
        RETURN_IF_ERROR(checked_size(freq_begin, has_freqs ? document_count : 0, &target_freqs));
        RETURN_IF_ERROR(checked_size(position_begin, position_count, &target_positions));
        RETURN_IF_ERROR(reserve_for_append(target_docids, target_freqs, target_positions));

        docids_.resize(target_docids);
        freqs_.resize(target_freqs);
        positions_flat_.resize(target_positions);
        destination->docids = std::span(docids_).subspan(doc_begin, document_count);
        destination->freqs = std::span(freqs_).subspan(freq_begin, has_freqs ? document_count : 0);
        destination->positions_flat =
                std::span(positions_flat_).subspan(position_begin, position_count);
        if (document_count != 0) {
            has_freqs_ = has_freqs;
        }
        return Status::OK();
    }

    // Appends one position while a source decodes a frequency-bearing fill.
    // The common path writes into retained capacity; growth keeps replacement
    // reservation accounting atomic.
    Status append_position(uint32_t position) {
        if (!has_freqs_.value_or(false)) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "term posting buffer: incremental positions require parallel freqs");
        }
        if (positions_flat_.size() == positions_flat_.capacity()) {
            size_t target_positions = 0;
            RETURN_IF_ERROR(checked_size(positions_flat_.size(), 1, &target_positions));
            RETURN_IF_ERROR(reserve_for_append(docids_.size(), freqs_.size(), target_positions));
        }
        positions_flat_.push_back(position);
        return Status::OK();
    }

    std::span<const uint32_t> docids() const { return docids_; }
    std::span<const uint32_t> freqs() const { return freqs_; }
    std::span<const uint32_t> positions_flat() const { return positions_flat_; }

private:
    static Status checked_size(size_t current, size_t additional, size_t* target) {
        if (additional > std::numeric_limits<size_t>::max() - current) {
            return Status::Error<ErrorCode::MEM_LIMIT_EXCEEDED, false>(
                    "term posting buffer: capacity overflow");
        }
        *target = current + additional;
        if (*target > std::numeric_limits<uint64_t>::max() / sizeof(uint32_t)) {
            return Status::Error<ErrorCode::MEM_LIMIT_EXCEEDED, false>(
                    "term posting buffer: byte capacity overflow");
        }
        return Status::OK();
    }

    static Status growth_capacity(size_t required, size_t current, size_t* target) {
        *target = required;
        if (current != 0 && current <= std::numeric_limits<size_t>::max() / 2) {
            *target = std::max(required, current * 2);
        }
        if (*target > std::numeric_limits<uint64_t>::max() / sizeof(uint32_t)) {
            return Status::Error<ErrorCode::MEM_LIMIT_EXCEEDED, false>(
                    "term posting buffer: growth capacity overflow");
        }
        return Status::OK();
    }

    static Status capacity_bytes(size_t docids_capacity, size_t freqs_capacity,
                                 size_t positions_capacity, uint64_t* bytes) {
        const uint64_t docids_bytes = docids_capacity * sizeof(uint32_t);
        const uint64_t freqs_bytes = freqs_capacity * sizeof(uint32_t);
        const uint64_t positions_bytes = positions_capacity * sizeof(uint32_t);
        if (freqs_bytes > std::numeric_limits<uint64_t>::max() - docids_bytes ||
            positions_bytes > std::numeric_limits<uint64_t>::max() - docids_bytes - freqs_bytes) {
            return Status::Error<ErrorCode::MEM_LIMIT_EXCEEDED, false>(
                    "term posting buffer: aggregate capacity overflow");
        }
        *bytes = docids_bytes + freqs_bytes + positions_bytes;
        return Status::OK();
    }

    Status reserve_for_append(size_t target_docids, size_t target_freqs, size_t target_positions) {
        const bool grow_docids = target_docids > docids_.capacity();
        const bool grow_freqs = target_freqs > freqs_.capacity();
        const bool grow_positions = target_positions > positions_flat_.capacity();
        if (!grow_docids && !grow_freqs && !grow_positions) {
            return Status::OK();
        }
        size_t docids_capacity = docids_.capacity();
        size_t freqs_capacity = freqs_.capacity();
        size_t positions_capacity = positions_flat_.capacity();
        if (grow_docids) {
            RETURN_IF_ERROR(growth_capacity(target_docids, docids_.capacity(), &docids_capacity));
        }
        if (grow_freqs) {
            RETURN_IF_ERROR(growth_capacity(target_freqs, freqs_.capacity(), &freqs_capacity));
        }
        if (grow_positions) {
            RETURN_IF_ERROR(growth_capacity(target_positions, positions_flat_.capacity(),
                                            &positions_capacity));
        }
        if (memory_reporter_ == nullptr) {
            if (grow_docids) docids_.reserve(docids_capacity);
            if (grow_freqs) freqs_.reserve(freqs_capacity);
            if (grow_positions) positions_flat_.reserve(positions_capacity);
            return Status::OK();
        }

        uint64_t previous_bytes = 0;
        uint64_t final_bytes = 0;
        RETURN_IF_ERROR(capacity_bytes(docids_.capacity(), freqs_.capacity(),
                                       positions_flat_.capacity(), &previous_bytes));
        RETURN_IF_ERROR(
                capacity_bytes(docids_capacity, freqs_capacity, positions_capacity, &final_bytes));
        RETURN_IF_ERROR(capacity_reservation_.set_bytes(final_bytes));

        const uint64_t overlap_bytes =
                std::max({grow_docids ? docids_.capacity() * sizeof(uint32_t) : 0,
                          grow_freqs ? freqs_.capacity() * sizeof(uint32_t) : 0,
                          grow_positions ? positions_flat_.capacity() * sizeof(uint32_t) : 0});
        MemoryReporter::Reservation overlap_reservation = memory_reporter_->make_reservation();
        Status overlap_status = overlap_reservation.set_bytes(overlap_bytes);
        if (!overlap_status.ok()) {
            DORIS_CHECK(capacity_reservation_.set_bytes(previous_bytes).ok());
            return overlap_status;
        }

        if (grow_docids) {
            docids_.reserve(docids_capacity);
            DCHECK_EQ(docids_.capacity(), docids_capacity);
        }
        if (grow_freqs) {
            freqs_.reserve(freqs_capacity);
            DCHECK_EQ(freqs_.capacity(), freqs_capacity);
        }
        if (grow_positions) {
            positions_flat_.reserve(positions_capacity);
            DCHECK_EQ(positions_flat_.capacity(), positions_capacity);
        }
        return Status::OK();
    }

    MemoryReporter* memory_reporter_ = nullptr;
    // The reservation precedes vectors so their allocations are destroyed first.
    MemoryReporter::Reservation capacity_reservation_;
    std::vector<uint32_t> docids_;
    std::vector<uint32_t> freqs_;
    std::vector<uint32_t> positions_flat_;
    std::optional<bool> has_freqs_;
};

class TermPostingSource {
public:
    virtual ~TermPostingSource() = default;

    // out is empty on entry. Unless this call reaches the term end, it must
    // return exactly target_docs postings. exhausted means no postings remain
    // after this call. The source and output are borrowed synchronously.
    virtual Status fill(uint32_t target_docs, TermPostingBuffer* out, bool* exhausted) = 0;
};

// Synchronous non-owning adapter for callers that already hold one materialized
// posting list. It slices the arrays into the writer's requested document
// windows without copying the whole term into an intermediate object.
class SpanTermPostingSource final : public TermPostingSource {
public:
    SpanTermPostingSource(std::span<const uint32_t> docids, std::span<const uint32_t> freqs,
                          std::span<const uint32_t> positions_flat)
            : docids_(docids), freqs_(freqs), positions_flat_(positions_flat) {}

    Status fill(uint32_t target_docs, TermPostingBuffer* out, bool* exhausted) override {
        if (out == nullptr || exhausted == nullptr || target_docs == 0) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "span posting source: invalid fill arguments");
        }
        if (!out->empty()) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "span posting source: output must be empty");
        }
        if (!freqs_.empty() && freqs_.size() != docids_.size()) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "span posting source: freqs length must equal docids");
        }
        if (freqs_.empty() && !positions_flat_.empty()) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "span posting source: positions require parallel freqs");
        }

        const size_t count =
                std::min(static_cast<size_t>(target_docs), docids_.size() - doc_offset_);
        size_t position_count = 0;
        if (!positions_flat_.empty()) {
            for (size_t i = 0; i < count; ++i) {
                RETURN_IF_ERROR(
                        checked_add(position_count, freqs_[doc_offset_ + i], &position_count));
            }
            if (position_count > positions_flat_.size() - position_offset_) {
                return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                        "span posting source: positions shorter than sum(freqs)");
            }
        }

        RETURN_IF_ERROR(out->append(
                docids_.subspan(doc_offset_, count),
                freqs_.empty() ? std::span<const uint32_t> {} : freqs_.subspan(doc_offset_, count),
                positions_flat_.subspan(position_offset_, position_count)));
        doc_offset_ += count;
        position_offset_ += position_count;
        *exhausted = doc_offset_ == docids_.size();
        if (*exhausted && !positions_flat_.empty() && position_offset_ != positions_flat_.size()) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "span posting source: positions longer than sum(freqs)");
        }
        return Status::OK();
    }

    bool exhausted() const { return doc_offset_ == docids_.size(); }

private:
    static Status checked_add(size_t current, uint32_t additional, size_t* result) {
        if (additional > std::numeric_limits<size_t>::max() - current) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "span posting source: position count overflow");
        }
        *result = current + additional;
        return Status::OK();
    }

    std::span<const uint32_t> docids_;
    std::span<const uint32_t> freqs_;
    std::span<const uint32_t> positions_flat_;
    size_t doc_offset_ = 0;
    size_t position_offset_ = 0;
};

struct StreamedTermPostings {
    std::string term;
    bool retain_positions = true;
    TermPostingSource* source = nullptr;
};

} // namespace doris::snii::writer
