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

#include "storage/index/inverted/spimi/freq_prox_encoder.h"
#include "storage/index/inverted/spimi/byte_output.h"
#include "storage/index/inverted/spimi/posting_buffer.h"
#include "storage/index/inverted/spimi/term_dict_writer.h"

namespace doris::segment_v2::inverted_index::spimi {

// Orchestrates emission of a single full-text index segment from a sorted
// SpimiPostingBuffer. Combines the per-stream writers (TermDictWriter for
// `.tii` / `.tis`, FreqProxEncoder for `.frq` / `.prx`) into a single
// `Emit()` call.
//
// Inputs (ByteOutputs) are owned by the caller; this class only writes to
// them. Typical wiring at the integration layer (Phase 7) is:
//
//   io::FileWriter file_tis, file_tii, file_frq, file_prx;
//   FileByteOutput out_tis(&file_tis), out_tii(&file_tii), ...;
//   SegmentWriter writer(&out_tis, &out_tii, &out_frq, &out_prx);
//   buffer.Sort();
//   writer.Emit(buffer, /*field_number=*/0);
//   writer.Close();
//
// For unit tests the ByteOutputs are typically `MemoryByteOutput`.
//
// Iteration assumes the records in `buffer` are already sorted by
// (term, doc, position) — i.e., the caller has invoked
// `SpimiPostingBuffer::Sort()`. Records sharing a `text_ref` are
// guaranteed to be contiguous after sort because Intern() dedupes terms,
// and the comparator orders distinct `text_ref` blocks by term bytes.
class SegmentWriter {
public:
    SegmentWriter(ByteOutput* tis_out, ByteOutput* tii_out, ByteOutput* frq_out,
                  ByteOutput* prx_out,
                  int32_t index_interval = TermDictWriter::kDefaultIndexInterval,
                  int32_t skip_interval = TermDictWriter::kDefaultSkipInterval,
                  int32_t max_skip_levels = TermDictWriter::kMaxSkipLevels,
                  bool omit_term_freq_and_positions = false);

    SegmentWriter(const SegmentWriter&) = delete;
    SegmentWriter& operator=(const SegmentWriter&) = delete;

    // Emits all terms in `buffer` under `field_number` into the four
    // outputs. Returns the number of distinct terms written. `buffer` must
    // have been Sort()-ed by the caller; the records inside are read but
    // not mutated.
    int64_t Emit(const SpimiPostingBuffer& buffer, int32_t field_number);

    // Writes term-dictionary footers. After Close(), the underlying outputs
    // hold a complete Lucene-format `.tis` / `.tii` / `.frq` / `.prx`
    // pair that the existing CLucene reader can consume.
    void Close();

    int64_t TermCount() const { return _term_count; }

private:
    TermDictWriter _dict;
    FreqProxEncoder _encoder;
    int64_t _term_count = 0;
    bool _closed = false;
};

} // namespace doris::segment_v2::inverted_index::spimi
