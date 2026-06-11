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
// path.  All input segments are successive slices of the SAME
// monotonically increasing _rid (doc_id) stream: the write path appends
// every token with the absolute row id and spills the buffer at row
// boundaries, so each segment already carries GLOBAL, absolute doc_ids
// whose ranges never overlap across inputs.  The merge therefore
// concatenates the already-ordered posting runs verbatim and must NOT
// apply any per-segment doc_id offset — doing so double-shifts every doc
// after the first spill and pushes doc_ids past total_doc_count (see
// SegmentMergerTest.MultiSpillAbsoluteDocIdsArePreserved).
//
// Inputs:
//   - N spill segments from SpillManager (each flushed when the
//     posting buffer exceeded the memory budget)
//   - Optionally, one "final" segment produced from the remaining
//     buffer at finish() time (its doc_ids are likewise global)
//
// Output: a single merged segment written to the caller-provided
// SpimiSegmentSink.
class SegmentMerger {
public:
    // One input segment.  All byte vectors must contain a complete
    // .tis/.tii/.frq/.prx as produced by SpimiFulltextWriter::
    // EmitSegment (or equivalently, SegmentWriter + TermDictWriter).
    // `doc_count` is the cumulative doc_count recorded when this segment was
    // spilled (max absolute doc_id + 1). It is retained as metadata only:
    // because inputs carry global absolute doc_ids, the merge does NOT use it to
    // offset doc_ids. The merged segment's doc_count is passed separately as
    // `total_doc_count`.
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
                         int32_t total_doc_count, int32_t index_version,
                         bool omit_term_freq_and_positions, bool omit_norms);
};

} // namespace doris::segment_v2::inverted_index::spimi
