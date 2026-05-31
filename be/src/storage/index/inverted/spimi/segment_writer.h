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

#include "storage/index/inverted/spimi/byte_output.h"
#include "storage/index/inverted/spimi/freq_prox_encoder.h"
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
    // Default byte budget for inlining a term's full posting (.frq + .prx)
    // bytes into its .tis entry. Small terms dominate a real (Zipf) vocabulary
    // so a byte budget captures most of the GET-count win while bounding .tis
    // bloat; measured on the ACTUAL encoded block sizes, not df.
    static constexpr uint32_t kInlineMaxBytes = 256;

    SegmentWriter(ByteOutput* tis_out, ByteOutput* tii_out, ByteOutput* frq_out,
                  ByteOutput* prx_out,
                  int32_t index_interval = TermDictWriter::kDefaultIndexInterval,
                  int32_t skip_interval = TermDictWriter::kDefaultSkipInterval,
                  int32_t max_skip_levels = TermDictWriter::kMaxSkipLevels,
                  bool omit_term_freq_and_positions = false,
                  // V4 windowed `.frq`/`.prx` framing (see WindowFrameEncoder).
                  bool use_windowed = false,
                  // When true, a term whose full posting (.frq + .prx) block is
                  // <= inline_threshold bytes is stored INLINE in the .tis entry
                  // (zero extra GET on read); larger terms keep external
                  // pointers + range-GET. The .tis switches to kFormatInline.
                  bool inline_small_terms = false, uint32_t inline_threshold = kInlineMaxBytes);

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
    // Emits from the flat `records()` vector (high-vocab flat mode, or
    // compact mode that required `_records` materialization).
    int64_t EmitFromRecords(const SpimiPostingBuffer& buffer, int32_t field_number);
    // Emits straight from the compact per-term arrays in text-sorted order,
    // used when `buffer.CompactDirectEmitReady()`. Avoids the `_records`
    // materialization but produces byte-identical output to EmitFromRecords.
    int64_t EmitFromCompactDirect(const SpimiPostingBuffer& buffer, int32_t field_number);

    // Closes the currently-open term: in inline mode, decides inline-vs-flush
    // on the staged block bytes and writes the .tis entry accordingly; in
    // non-inline mode the encoder already wrote the block to the real outputs,
    // so this just adds the external .tis entry. Shared by both emit paths.
    void FinishAndAddTerm(int32_t field_number, std::string_view term_text);

    ByteOutput* _frq_out; // real .frq output (advanced here when a term is external)
    ByteOutput* _prx_out; // real .prx output (advanced here when a term is external)
    bool _inline_small_terms;
    uint32_t _inline_threshold;

    TermDictWriter _dict;
    FreqProxEncoder _encoder;
    int64_t _term_count = 0;
    bool _closed = false;
};

} // namespace doris::segment_v2::inverted_index::spimi
