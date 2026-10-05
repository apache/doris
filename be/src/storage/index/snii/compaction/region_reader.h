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

#include <array>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/io/file_reader.h"
#include "storage/index/snii/writer/memory_reporter.h"

// SequentialRegionReader -- chunked sequential read-ahead over ONE contiguous
// byte region of a source file (T2.3, compaction index-merge fast path).
//
// The merge walks a source segment's posting region in ascending offset order
// (the writer laid the per-term [prx][frq] spans out in term order, and the
// term cursor replays terms in that same order), so per-window read_at calls
// would issue thousands of tiny reads over an already-sequential byte stream.
// This reader amortizes them: resolve() serves a window from the buffered
// chunk when possible and only touches the file on a miss.
//
// resolve() preference order (documented contract, pinned by UT):
//   1. window fully inside the buffered chunk -> zero-copy Slice into the
//      buffer, NO file read;
//   2. forward miss with len <= chunk_bytes -> ONE chunk read starting at the
//      window (clamped to the region end, never past it) and a slice of it;
//   3. oversized (len > chunk_bytes) or backward window -> ONE exact range
//      read into *scratch, leaving the buffered chunk untouched (a rare
//      backward probe must not thrash the forward stream).
// A window not fully inside [region_offset, region_offset+region_length) is
// Corruption -- posting locators were already validated against the region by
// LogicalIndexReader::resolve_*_window, so an out-of-region request here means
// a caller bug or corrupt state, never a legal miss.
//
// The returned Slice is valid until the NEXT resolve() call (buffer path) or
// until *scratch is next modified (fallback path); callers decode immediately.
// Single-threaded, borrowed FileReader must outlive the region reader.
namespace doris::snii::compaction {

enum class PostingStream : uint8_t { kDocs = 0, kPrx = 1 };

// Two logical monotone posting streams share two aligned physical chunks. A
// stream pins at most its current chunk, so resolving the other stream cannot
// invalidate an outstanding Slice. Requests crossing an aligned chunk use the
// caller's per-stream scratch and do not evict either pinned chunk.
class SharedAlignedRegionCache {
public:
    SharedAlignedRegionCache(io::FileReader* reader, uint64_t region_offset, uint64_t region_length,
                             size_t total_budget_bytes,
                             writer::MemoryReporter* memory_reporter = nullptr)
            : reader_(reader),
              region_off_(region_offset),
              region_len_(region_length),
              total_budget_bytes_(total_budget_bytes),
              memory_reporter_(memory_reporter) {}

    Status init();
    Status resolve(PostingStream stream, uint64_t abs_off, uint64_t len,
                   std::vector<uint8_t>* scratch, Slice* out,
                   writer::MemoryReporter::Reservation* scratch_reservation = nullptr);

    uint64_t physical_read_ranges() const { return physical_read_ranges_; }
    uint64_t physical_read_bytes() const { return physical_read_bytes_; }
    uint64_t read_calls(PostingStream stream) const;
    uint64_t buffer_hits(PostingStream stream) const;
    size_t resident_capacity_bytes() const;

private:
    static constexpr size_t kStreamCount = 2;
    static constexpr size_t kSlotCount = 2;

    struct Slot {
        std::vector<uint8_t> bytes;
        uint64_t offset = 0;
        uint8_t pins = 0;
        bool valid = false;
    };

    static size_t stream_index(PostingStream stream);
    void unpin(PostingStream stream);
    Status read_physical(PostingStream stream, uint64_t abs_off, size_t len,
                         std::vector<uint8_t>* out);

    io::FileReader* reader_ = nullptr;
    uint64_t region_off_ = 0;
    uint64_t region_len_ = 0;
    size_t total_budget_bytes_ = 0;
    size_t block_bytes_ = 0;
    writer::MemoryReporter* memory_reporter_ = nullptr;
    std::array<writer::MemoryReporter::Reservation, kSlotCount> slot_reservations_;
    std::array<Slot, kSlotCount> slots_;
    std::array<int8_t, kStreamCount> stream_slots_ {-1, -1};
    std::array<uint64_t, kStreamCount> read_calls_ {};
    std::array<uint64_t, kStreamCount> buffer_hits_ {};
    uint64_t physical_read_ranges_ = 0;
    uint64_t physical_read_bytes_ = 0;
    bool initialized_ = false;
};

class SequentialRegionReader {
public:
    // Default chunk: large enough to amortize per-window read overhead over
    // slim terms, small enough that k sources x 1 chunk stays negligible next
    // to the merge's memory-precheck budget. Configurable per instance (the
    // compaction wiring exposes a config knob in T2.6).
    static constexpr size_t kDefaultChunkBytes = 4ULL << 20;

    SequentialRegionReader(io::FileReader* reader, uint64_t region_offset, uint64_t region_length,
                           size_t chunk_bytes = kDefaultChunkBytes)
            : reader_(reader),
              region_off_(region_offset),
              region_len_(region_length),
              chunk_bytes_(chunk_bytes == 0 ? kDefaultChunkBytes : chunk_bytes) {}

    // Resolves the absolute byte window [abs_off, abs_off+len) per the
    // contract above. len == 0 yields an empty slice without touching the
    // file.
    Status resolve(uint64_t abs_off, uint64_t len, std::vector<uint8_t>* scratch, Slice* out);

    // Observability for tests/profiling: physical reads issued vs windows
    // served straight from the buffered chunk.
    uint64_t read_calls() const { return read_calls_; }
    uint64_t buffer_hits() const { return buffer_hits_; }

private:
    io::FileReader* reader_ = nullptr;
    uint64_t region_off_ = 0;
    uint64_t region_len_ = 0;
    size_t chunk_bytes_ = kDefaultChunkBytes;

    std::vector<uint8_t> buf_; // buffered chunk; empty until the first fill
    uint64_t buf_off_ = 0;     // absolute offset of buf_[0] (valid when buf_ non-empty)

    uint64_t read_calls_ = 0;
    uint64_t buffer_hits_ = 0;
};

} // namespace doris::snii::compaction
