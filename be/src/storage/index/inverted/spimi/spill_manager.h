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
#include <string>
#include <vector>

#include "storage/index/inverted/spimi/posting_buffer.h"

namespace doris::segment_v2::inverted_index::spimi {

// One spill segment flushed from the posting buffer.
// All byte vectors contain a complete Lucene 2.x segment
// (.tis/.tii/.frq/.prx/.fnm + segments_N/segments.gen) that the
// SegmentMerger will later read back for the k-way merge.
struct SpillSegment {
    std::string segment_name; // "_spill_0", "_spill_1", ...
    int32_t doc_count = 0;    // max doc_id + 1 in this spill
    int64_t term_count = 0;   // distinct terms emitted

    std::vector<uint8_t> tis_bytes;
    std::vector<uint8_t> tii_bytes;
    std::vector<uint8_t> frq_bytes;
    std::vector<uint8_t> prx_bytes;
    std::vector<uint8_t> fnm_bytes;
    std::vector<uint8_t> segments_n_bytes;
    std::vector<uint8_t> segments_gen_bytes;
};

// Manages the spill-to-disk (here: spill-to-memory) lifecycle of the
// SPIMI posting buffer.  When the buffer's `ShouldFlush()` fires
// (memory budget exceeded), the integration layer calls
// `FlushBuffer()`, which sorts the buffer, emits a complete Lucene
// segment into in-memory byte vectors, and resets the buffer for the
// next batch.
//
// At finish() time the integration layer collects all spill segments
// (plus any remaining buffer contents) and hands them to the
// SegmentMerger for the final k-way merge.
class SpillManager {
public:
    // `is_v4` makes spill segments use kIndexVersionV4 (windowed+inline-capable)
    // in lockstep with the final segment's .fnm. This MUST match the final
    // EmitDirect/EmitMerged index_version so SegmentMerger::MergeSingleInput's
    // byte-copy of a spill's .frq/.prx stays format-consistent with the .fnm it
    // rewrites. Adaptive per-term windowing keeps the df=1 tail legacy either way.
    explicit SpillManager(std::string field_name, bool is_v4 = false);

    SpillManager(const SpillManager&) = delete;
    SpillManager& operator=(const SpillManager&) = delete;

    // Sorts `buffer`, emits its contents as a complete Lucene segment
    // into in-memory byte vectors, and calls `buffer.Reset()`.
    // `doc_count` is the number of documents the segment covers
    // (typically `_spimi_doc_count` from the integration layer).
    //
    // Returns the number of distinct terms emitted.
    // After this call, `buffer` is empty and ready for new Appends.
    int64_t FlushBuffer(SpimiPostingBuffer& buffer, int32_t doc_count);

    size_t SpillCount() const { return _spills.size(); }
    const std::vector<SpillSegment>& Spills() const { return _spills; }

    // Releases all in-memory spill segment data.  Called after the
    // SegmentMerger has produced the final merged segment.
    void CleanupSpillFiles();

    // Total bytes across all spill segments (approximate memory used
    // by spilled data).
    size_t TotalSpillBytes() const;

private:
    std::string _field_name;
    bool _is_v4 = false;
    std::vector<SpillSegment> _spills;
    int32_t _spill_counter = 0;
};

} // namespace doris::segment_v2::inverted_index::spimi
