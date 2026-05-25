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
#include <memory>
#include <vector>

#include "storage/index/inverted/spimi/lucene_output.h"

namespace doris::segment_v2::inverted_index::spimi {

// Multi-level skip-list writer for the Lucene 2.x posting list format,
// reimplemented in Doris so the SPIMI segment emitter does not link against
// CLucene. The byte layout is identical to CLucene's
// `MultiLevelSkipListWriter` + `DefaultSkipListWriter` (with `hasProx=true,
// storePayloads=false, indexVersion=kV0`), so the existing CLucene reader
// (`SegmentTermDocs::skipTo`) consumes it without modification.
//
// Layout (CLucene SkipListWriter):
//   .frq tail-block following a term's docs/freq stream:
//     for level = N-1 down to 1:
//       vlong  level_byte_length
//       bytes  level_payload
//     bytes  level_0_payload    // implicit length = remaining bytes
//
//   Each level's payload is a sequence of N entries (one per skip-interval
//   block at that level). Each entry encodes:
//     vint   doc_delta             (curDoc - lastSkipDoc[level])
//     vint   freq_pointer_delta    (curFreqPointer - lastSkipFreqPointer)
//     vint   prox_pointer_delta    (curProxPointer - lastSkipProxPointer)
//   Levels > 0 additionally append:
//     vlong  child_pointer         (byte offset into level-1's buffer at the
//                                   matching skip-block)
//
// Usage (per term):
//   skip_list_writer.Reset(df, frq_pointer_at_term_start,
//                          prox_pointer_at_term_start);
//   for (each doc in the term):
//     if (doc_index > 0 && doc_index % skip_interval == 0) {
//       skip_list_writer.SetSkipData(prev_doc, curFreqPointer, curProxPointer);
//       skip_list_writer.BufferSkip(doc_index);
//     }
//     // ... write doc/freq/positions to frq/prox ...
//   int64_t skip_pointer = skip_list_writer.WriteSkip(frq_out);
//   int32_t skip_offset = static_cast<int32_t>(skip_pointer - term_frq_start);
//
// The writer is stateful and reusable across terms via Reset(). It does not
// own the freq/prox outputs.
class SkipListWriter {
public:
    SkipListWriter(int32_t skip_interval, int32_t max_skip_levels);

    SkipListWriter(const SkipListWriter&) = delete;
    SkipListWriter& operator=(const SkipListWriter&) = delete;

    // Begin emitting skip lists for a new term. `df` is the term's document
    // frequency; it determines how many skip levels are needed.
    // `freq_pointer_start` and `prox_pointer_start` are the .frq / .prx byte
    // offsets at the term's first doc — they are the baselines from which
    // per-level pointer deltas are measured.
    void Reset(int32_t df, int64_t freq_pointer_start, int64_t prox_pointer_start);

    // Sets the data for the next BufferSkip() call. `doc` is the doc id
    // appearing immediately *before* a new skip block begins; the freq /
    // prox pointers are the .frq / .prx byte offsets at that boundary.
    void SetSkipData(int32_t doc, int64_t freq_pointer, int64_t prox_pointer);

    // Emits the skip entry currently held via SetSkipData() into the
    // appropriate level buffers. `df` is the document index this call
    // corresponds to (which determines how many levels are written: a doc
    // index that is a multiple of skip_interval^(L+1) bubbles up to level L).
    void BufferSkip(int32_t df);

    // Flushes all buffered skip levels into `out`. Returns the file offset
    // of the first byte written (CLucene's "skipPointer"); callers compute
    // the per-term `skip_offset` as `skip_pointer - term_frq_start`.
    int64_t WriteSkip(LuceneOutput* out);

    int32_t NumberOfSkipLevels() const { return _number_of_skip_levels; }
    int32_t SkipInterval() const { return _skip_interval; }

private:
    // Writes one skip entry at `level` into `level_buf`. Does not write the
    // child pointer (caller appends it for level > 0).
    void WriteSkipEntry(int32_t level, MemoryLuceneOutput* level_buf);

    void EnsureLevels(int32_t levels);

    int32_t _skip_interval;
    int32_t _max_skip_levels;

    int32_t _number_of_skip_levels = 0;
    int32_t _cur_doc = 0;
    int64_t _cur_freq_pointer = 0;
    int64_t _cur_prox_pointer = 0;

    std::vector<int32_t> _last_skip_doc;
    std::vector<int64_t> _last_skip_freq_pointer;
    std::vector<int64_t> _last_skip_prox_pointer;
    std::vector<std::unique_ptr<MemoryLuceneOutput>> _skip_buffers;
};

} // namespace doris::segment_v2::inverted_index::spimi
