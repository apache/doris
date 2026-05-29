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

#include "storage/index/inverted/spimi/byte_output.h"
#include "storage/index/inverted/spimi/fulltext_writer.h"

namespace doris::segment_v2::inverted_index::spimi {

// K-way merge of multiple Lucene 2.x segments produced by the spill
// path.  Each input segment has its own local doc_id space (0-based);
// the merger applies a per-segment offset so the merged output has
// globally monotonic doc_ids.
//
// Inputs:
//   - N spill segments from SpillManager (each flushed when the
//     posting buffer exceeded the memory budget)
//   - Optionally, one "final" segment produced from the remaining
//     buffer at finish() time (its doc_ids are already global, so
//     the caller adjusts its doc_count accordingly)
//
// Output: a single merged segment written to the caller-provided
// SpimiSegmentSink.
class SegmentMerger {
public:
    // One input segment.  All byte vectors must contain a complete
    // .tis/.tii/.frq/.prx as produced by SpimiFulltextWriter::
    // EmitSegment (or equivalently, SegmentWriter + TermDictWriter).
    // `doc_count` is the number of documents this segment covers
    // (max doc_id + 1 in local space).
    struct Input {
        std::vector<uint8_t> tis_bytes;
        std::vector<uint8_t> tii_bytes;
        std::vector<uint8_t> frq_bytes;
        std::vector<uint8_t> prx_bytes;
        int32_t doc_count = 0;
    };

    // Merges `inputs` into a single output segment.
    //
    // `sink`: the ByteOutput streams for the merged segment.
    // `segment_name`: written into the segments_N manifest.
    // `field_name`: written into the .fnm.
    // `total_doc_count`: the merged segment's doc_count (typically
    //   `_spimi_doc_count` from the integration layer).
    // `index_version`: field-info version tag.
    // `omit_term_freq_and_positions`: field-level flag propagated
    //   to the encoder and .fnm.
    // `omit_norms`: field-level flag propagated to .fnm.
    //
    // Returns the number of distinct terms in the merged segment.
    static int64_t Merge(const std::vector<Input>& inputs, const SpimiSegmentSink& sink,
                         const std::string& segment_name, const std::string& field_name,
                         int32_t total_doc_count,
                         int32_t index_version,
                         bool omit_term_freq_and_positions, bool omit_norms);
};

} // namespace doris::segment_v2::inverted_index::spimi
