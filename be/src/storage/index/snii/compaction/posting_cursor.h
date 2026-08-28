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
#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/compaction/region_reader.h"
#include "storage/index/snii/compaction/rowid_conversion.h"
#include "storage/index/snii/format/core_metadata.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/format/frq_prelude.h"
#include "storage/index/snii/format/prx_decode_stats.h"
#include "storage/index/snii/reader/logical_index_reader.h"

namespace doris::snii::compaction {

struct DestinationPostingRun {
    uint32_t destination_segment = 0;
    uint32_t document_end = 0;
};

// One destination-homogeneous slice of a decoded physical chunk. Positioned
// runs retain the decoder's offset base; positions_flat covers exactly
// [position_offsets.front(), position_offsets.back()).
struct RemappedPostingChunk {
    uint32_t destination_segment = 0;
    std::span<const uint32_t> destination_docids;
    std::span<const uint32_t> freqs;
    std::span<const uint32_t> position_offsets;
    std::span<const uint32_t> positions_flat;
};

// Checks [region.offset, region.offset + region.length) against the source
// file without ever evaluating the potentially-overflowing addition.
Status validate_posting_region(const format::RegionRef& region, uint64_t file_size);
bool posting_entry_has_positions(const format::DictEntry& entry);

class SniiPostingCursor;

// Persistent read/decode state for every term decoded from one source logical
// index. Docs and positions share one bounded physical chunk cache while their
// decoder vectors and prelude workspace survive term cursor destruction.
class SniiPostingReadContext {
public:
    static constexpr size_t kMaxReadAheadBudgetBytes =
            2 * SequentialRegionReader::kDefaultChunkBytes;
    static constexpr size_t kMaxRetainedDecoderWorkspaceBytes = 64ULL << 10;

    SniiPostingReadContext(const reader::LogicalIndexReader* index,
                           size_t total_read_ahead_budget_bytes,
                           writer::MemoryReporter* memory_reporter = nullptr)
            : index_(index),
              total_read_ahead_budget_bytes_(total_read_ahead_budget_bytes),
              memory_reporter_(memory_reporter) {}

    SniiPostingReadContext(const SniiPostingReadContext&) = delete;
    SniiPostingReadContext& operator=(const SniiPostingReadContext&) = delete;
    SniiPostingReadContext(SniiPostingReadContext&&) = delete;
    SniiPostingReadContext& operator=(SniiPostingReadContext&&) = delete;

    Status init();

    const reader::LogicalIndexReader* index() const { return index_; }
    bool initialized() const { return initialized_; }
    size_t total_read_ahead_budget_bytes() const { return total_read_ahead_budget_bytes_; }
    uint64_t docs_read_calls() const;
    uint64_t prx_read_calls() const;
    uint64_t docs_buffer_hits() const;
    uint64_t prx_buffer_hits() const;
    uint64_t physical_read_ranges() const;
    uint64_t physical_read_bytes() const;
    size_t resident_read_ahead_capacity_bytes() const;
    size_t decoder_workspace_capacity_bytes() const;
    size_t retained_decoder_workspace_limit_bytes() const {
        return std::min(total_read_ahead_budget_bytes_, kMaxRetainedDecoderWorkspaceBytes);
    }

private:
    friend class SniiPostingCursor;

    struct DecoderWorkspace final : public format::PrxCsrAllocationGate {
        format::FrqPreludeReader prelude;
        writer::MemoryReporter::Reservation docs_scratch_reservation;
        writer::MemoryReporter::Reservation prx_scratch_reservation;
        writer::MemoryReporter::Reservation docids_reservation;
        writer::MemoryReporter::Reservation positions_reservation;
        writer::MemoryReporter::Reservation position_offsets_reservation;
        writer::MemoryReporter::Reservation destination_runs_reservation;
        writer::MemoryReporter::Reservation frequencies_reservation;
        writer::MemoryReporter::Reservation decompressed_reservation;
        std::vector<uint8_t> docs_scratch;
        std::vector<uint8_t> prx_scratch;
        std::vector<uint8_t> decompressed;
        std::vector<uint32_t> docids;
        std::vector<uint32_t> positions_flat;
        std::vector<uint32_t> position_offsets;
        std::vector<DestinationPostingRun> destination_runs;
        std::vector<uint32_t> frequencies;
        bool reservations_enabled = false;

        size_t capacity_bytes() const;
        void init_memory_reporter(writer::MemoryReporter* memory_reporter);
        Status reserve_docids(size_t count);
        Status reserve_remapped(size_t document_count, size_t run_count, bool retain_frequencies);
        Status reserve_csr(std::vector<uint32_t>* pos_flat, size_t position_count,
                           std::vector<uint32_t>* pos_off, size_t offset_count) override;
        Status reserve_decompression(size_t bytes, std::vector<uint8_t>** buffer) override;
        void release_large_buffers(size_t retained_capacity_limit_bytes);
    };

    class TermLease {
    public:
        ~TermLease();

        TermLease(const TermLease&) = delete;
        TermLease& operator=(const TermLease&) = delete;

    private:
        friend class SniiPostingReadContext;
        explicit TermLease(SniiPostingReadContext* context) : context_(context) {}
        SniiPostingReadContext* context_ = nullptr;
    };

    Status acquire_term(bool has_docs_range, bool has_prx_range,
                        const format::RegionRef& docs_range, const format::RegionRef& prx_range,
                        std::unique_ptr<TermLease>* lease);
    Status validate_next_range(const format::RegionRef& range, bool has_previous,
                               uint64_t previous_end, const char* stream, uint64_t* end) const;
    Status poison(Status status);
    Status poison_active_term(Status status, TermLease* lease);
    void release_term();
    const Status& failed_status() const { return failed_; }

    const reader::LogicalIndexReader* index_ = nullptr;
    size_t total_read_ahead_budget_bytes_ = 0;
    writer::MemoryReporter* memory_reporter_ = nullptr;
    format::RegionRef posting_region_;
    std::unique_ptr<SharedAlignedRegionCache> posting_cache_;
    DecoderWorkspace decoder_workspace_;

    uint64_t last_docs_end_ = 0;
    uint64_t last_prx_end_ = 0;
    bool has_docs_range_ = false;
    bool has_prx_range_ = false;
    bool term_active_ = false;
    bool initialized_ = false;
    Status failed_ = Status::OK();
};

// Sequential positions-posting decoder for one term in one source SNII index.
// It accepts all v1 physical shapes (inline, slim POD-ref and windowed POD-ref),
// validates the decoded doc/frequency/position stream, applies a validated
// row-id conversion, and yields surviving rows in destination-order chunks.
//
// The borrowed read context and row-id capability must outlive the cursor. A
// cursor is single-use and single-threaded. Only one cursor may hold a context
// term lease at a time.
class SniiPostingCursor {
public:
    SniiPostingCursor(SniiPostingReadContext* read_context, format::DictEntry entry,
                      uint64_t frq_base, uint64_t prx_base, uint32_t source_ordinal,
                      const ValidatedRowIdConversion* rowid_conversion)
            : read_context_(read_context),
              index_(read_context == nullptr ? nullptr : read_context->index()),
              entry_(std::move(entry)),
              frq_base_(frq_base),
              prx_base_(prx_base),
              source_ordinal_(source_ordinal),
              rowid_conversion_(rowid_conversion),
              term_has_positions_(posting_entry_has_positions(entry_)) {}

    // Validates index capability, posting locators and fixed entry geometry,
    // then acquires the source context's exclusive term lease. Payload decoding
    // is lazy: corrupt DD/PRX frames surface from next_chunk().
    Status init();

    // Returns the next non-empty remapped chunk. Its spans, including an exact
    // base-relative positions slice, remain valid until the next call.
    // Decoder/corruption failures poison the cursor and are returned unchanged
    // by subsequent calls.
    Status next_chunk(RemappedPostingChunk* chunk, bool* has_chunk);
    bool has_positions() const { return term_has_positions_; }

private:
    enum class Shape : uint8_t { kFlat, kWindowed };

    Status validate_entry_geometry();
    Status prepare_flat_ranges();
    Status prepare_windowed_ranges();
    Status prepare_windowed();
    Status load_next_chunk(bool* loaded);
    Status load_flat_chunk();
    Status load_windowed_chunk();
    Status decode_dd(Slice bytes, const format::FrqRegionMeta& meta, uint64_t win_base,
                     uint32_t expected_doc_count);
    Status decode_prx(Slice bytes, format::PrxDecodedShape* shape);
    Status map_decoded_chunk();
    void emit_next_mapped_run(RemappedPostingChunk* chunk);
    Status finish_source();
    Status poison(Status status);

    SniiPostingReadContext* read_context_ = nullptr;
    const reader::LogicalIndexReader* index_ = nullptr;
    format::DictEntry entry_;
    uint64_t frq_base_ = 0;
    uint64_t prx_base_ = 0;
    uint32_t source_ordinal_ = 0;
    const ValidatedRowIdConversion* rowid_conversion_ = nullptr;
    std::span<const std::pair<uint32_t, uint32_t>> source_mapping_;
    bool source_has_deletions_ = false;
    bool term_has_positions_ = true;

    Shape shape_ = Shape::kFlat;
    std::unique_ptr<SniiPostingReadContext::TermLease> term_lease_;
    SniiPostingReadContext::DecoderWorkspace* workspace_ = nullptr;

    uint64_t flat_dd_abs_ = 0;
    uint64_t flat_dd_len_ = 0;
    uint64_t flat_prx_abs_ = 0;
    uint64_t flat_prx_len_ = 0;
    uint32_t next_window_ = 0;
    bool flat_loaded_ = false;

    uint64_t decoded_docs_ = 0;
    uint64_t decoded_total_freq_ = 0;
    uint32_t decoded_max_freq_ = 0;
    size_t next_destination_run_ = 0;

    bool initialized_ = false;
    bool exhausted_ = false;
    Status failed_ = Status::OK();
};

} // namespace doris::snii::compaction
