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

namespace doris::segment_v2::inverted_index::spimi {

// One entry in the `segments_N` manifest. Mirrors CLucene's `SegmentInfo`
// for the parameters Doris uses (no deletes, no shared doc store, no
// separate norms files).
struct SegmentInfoEntry {
    std::string name;              // e.g. "_0"
    int32_t doc_count = 0;         // # docs in the segment
    int64_t del_gen = -1;          // -1 = no deletes
    int32_t doc_store_offset = -1; // -1 = no shared doc store
    std::string doc_store_segment; // ignored when offset == -1
    bool doc_store_is_compound_file = false;
    bool has_single_norm_file = true; // CLucene defaults to .nrm
    int8_t is_compound_file = 1;      // 1 = .cfs/.cfx, -1 = no compound
};

// Reimplements CLucene's `SegmentInfos::write` byte-for-byte for the
// SHARED_DOC_STORE format (CURRENT_FORMAT = -4). Writes both
// `segments_N` (full manifest) and `segments.gen` (redundancy pointer
// to the current generation) so the existing reader can discover the
// segment via either route.
//
// Format of `segments_N` at FORMAT = -4:
//   int    -4                       (CURRENT_FORMAT)
//   long   version                  (monotonic write-counter)
//   int    counter                  (next segment id; we use segment_count)
//   int    segment_count
//   per segment:
//     vint+wchars  name
//     int          doc_count
//     long         del_gen          (-1 ⇒ no deletes)
//     int          doc_store_offset (-1 ⇒ no shared store)
//     [if offset != -1]
//       vint+wchars  doc_store_segment
//       byte         doc_store_is_compound_file
//     byte         has_single_norm_file
//     int          normGen.length   (-1 ⇒ no norms, NO sentinel)
//     [per norm]   long normGen[j]
//     byte         is_compound_file
//
// Format of `segments.gen`:
//   int    -2                       (FORMAT_LOCKLESS)
//   long   generation
//   long   generation               (written twice for redundancy)
class SegmentInfosWriter {
public:
    static constexpr int32_t kFormatSharedDocStore = -4;
    static constexpr int32_t kFormatLockless = -2;
    static constexpr int32_t kNoNormGen = -1;

    SegmentInfosWriter() = default;

    SegmentInfosWriter(const SegmentInfosWriter&) = delete;
    SegmentInfosWriter& operator=(const SegmentInfosWriter&) = delete;

    // Writes a `segments_N` payload to `out`. `version` advances on every
    // write; `counter` is CLucene's "next segment id" — for our single-
    // segment SPIMI flush it equals `segments.size()`.
    void WriteSegmentsN(ByteOutput* out, int64_t version, int32_t counter,
                        const std::vector<SegmentInfoEntry>& segments) const;

    // Writes a `segments.gen` payload to `out`. `generation` is the
    // "_N" suffix of the latest `segments_N`.
    void WriteSegmentsGen(ByteOutput* out, int64_t generation) const;

private:
    static void WriteWideString(ByteOutput* out, const std::string& utf8);
    static void WriteSegment(ByteOutput* out, const SegmentInfoEntry& seg);
};

} // namespace doris::segment_v2::inverted_index::spimi
