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

#include "storage/index/inverted/spimi/segment_infos_writer.h"

namespace doris::segment_v2::inverted_index::spimi {

// Reader for the `segments_N` manifest produced by
// `SegmentInfosWriter::WriteSegmentsN`. Used by the upcoming
// `SpimiFulltextIndexReader` (P37c-2f) to discover the segment's
// `doc_count`, which serves as the `max_doc` parameter for
// `SpimiQueryIndexReader`.
//
// Format inverse of `SegmentInfosWriter::WriteSegment`:
//   int  -4 (kFormatSharedDocStore)
//   long version
//   int  counter
//   int  segment_count
//   per segment: name (vint+schars), doc_count, del_gen,
//                doc_store_offset, [doc_store_segment + flag],
//                has_single_norm_file, normGen.length [+ normGen[..]],
//                is_compound_file
class SegmentInfosReader {
public:
    struct Manifest {
        int64_t version = 0;
        int32_t counter = 0;
        std::vector<SegmentInfoEntry> segments;
    };

    // Parses the full manifest bytes; throws via DCHECK on a malformed
    // file. Caller-side production code should wrap in try/catch once
    // the reader is wired into the query path.
    static Manifest Read(const std::vector<uint8_t>& bytes);
};

} // namespace doris::segment_v2::inverted_index::spimi
