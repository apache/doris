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
#include <functional>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "gen_cpp/segment_v2.pb.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_writer.h"
#include "util/slice.h"

namespace doris::segment_v2 {
// -----------------------------------------------------------------------------
// External Column Meta layout and footer pointers
// -----------------------------------------------------------------------------
//
// Segment tail:
//
//   ┌──────────────────────────────────────────────────────────────────────┐
//   │ ... data pages ...                                                   │
//   │                                                                      │
//   │ Column Meta Region (variable-size, per top-level column, col_id asc) │
//   │   [Meta[col0]] [Meta[col1]] ... [Meta[colN-1]]                       │
//   │                                                                      │
//   │ Footer (SegmentFooterPB)                                             │
//   │   fields:                                                            │
//   │     - col_meta_region_start      : uint64                            │
//   │     - column_meta_entries        : repeated ColumnMetaEntryPB        │
//   │           - unique_id            : int32                             │
//   │           - length               : uint32                            │
//   │     - num_columns                : uint32                            │
//   └──────────────────────────────────────────────────────────────────────┘
//

// Wrapper class to expose static methods for external column meta utilities.
class ExternalColMetaUtil {
public:
    struct ExternalMetaPointers {
        uint64_t region_start = 0;
        // region_end is one-past-the-end file offset of the Column Meta Region.
        uint64_t region_end = 0;
        uint32_t num_columns = 0;
    };

    // Parse three external meta pointers from Footer.
    // Returns Status::OK() on success, or Status::Corruption() with detailed error message.
    static Status parse_external_meta_pointers(const SegmentFooterPB& footer,
                                               ExternalMetaPointers* out);

    // Parse uid->col_id mapping from footer.column_meta_entries.
    // Returns Status::OK() on success, or Status::Corruption() with detailed error message.
    static Status parse_uid_to_colid_map(const SegmentFooterPB& footer,
                                         const ExternalMetaPointers& ptrs,
                                         std::unordered_map<int32_t, size_t>* out);

    static bool is_valid_meta_slice(uint64_t pos, uint64_t size, const ExternalMetaPointers& p);

    // Read one ColumnMetaPB from external Column Meta Region; use INDEX cache lane because
    // it's small metadata. Column offsets are derived from footer.column_meta_lengths via
    // prefix sums.
    static Status read_col_meta(const io::FileReaderSPtr& file_reader,
                                const SegmentFooterPB& footer, const ExternalMetaPointers& p,
                                uint32_t col_id, ColumnMetaPB* out_meta);

    // Write external ColumnMetaPB contiguous region + footer pointers + column_meta_entries.
    // write_cb: a callback that appends slices to the underlying file (e.g., SegmentWriter::_write_raw_data).
    static Status write_external_column_meta(
            io::FileWriter* file_writer, SegmentFooterPB* footer,
            CompressionTypePB compression_type,
            const std::function<Status(const std::vector<Slice>&)>& write_cb);
};

} // namespace doris::segment_v2
