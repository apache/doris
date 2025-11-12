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
// External Column Meta (CMO) layout and footer pointers
// -----------------------------------------------------------------------------
//
// Segment tail (not to scale):
//
//   ┌──────────────────────────────────────────────────────────────────────┐
//   │ ... data pages ...                                                   │
//   │                                                                      │
//   │ Column Meta Region (variable-size, per top-level column, col_id asc) │
//   │   [Meta[col0]] [Meta[col1]] ... [Meta[colN-1]]                       │
//   │                                                                      │
//   │ CMO Table (N entries, 16 bytes each, little-endian)                  │
//   │   entry[i] := u64 pos_i | u64 size_i                                 │
//   │   // pos_i is absolute file offset of Meta[col i], size_i is length  │
//   │                                                                      │
//   │ Footer (SegmentFooterPB)                                             │
//   │   fields:                                                            │
//   │     - col_meta_region_start      : u64 (LE)                          │
//   │     - column_meta_offsets_start  : u64 (LE)                          │
//   │     - num_columns                : u32 (LE)                          │
//   │   fields:                                                            │
//   │     - top_level_column_uids      : repeated int32, in col_id order    │
//   └──────────────────────────────────────────────────────────────────────┘
//
// Read path (on-demand meta):
//   1) Parse the three pointers (region_start / cmo_offset / num_columns) from footer
//   2) For a given col_id, read the CMO entry at cmo_offset + col_id*16 to get (pos,size),
//      then validate pos/size within [region_start, cmo_offset) with overflow guard
//   3) pread(pos, size) → ParseFromArray(ColumnMetaPB)
//
// uid2colid:
//   - Used to check whether a top-level unique-id exists in this segment and to resolve its
//     column index (col_id) for CMO addressing
//   - NOT including variant subcolumns:
//       - non-sparse subcolumns are resolved via VariantExternalMetaReader (path→ColumnMetaPB)
//       - sparse subcolumns are embedded into the root variant's ColumnMetaPB.children_columns
//
// Write order (crash-consistent: no visible state without footer):
//   0) Variant pre-processing:
//        - externalize non-sparse subcolumns as KV (path→ColumnMetaPB bytes)
//        - embed sparse subcolumns into the root variant's ColumnMetaPB.children_columns
//        - footer.columns() retains only top-level columns (including variant roots with children)
//   1) Write Column Meta Region
//   2) Write CMO table
//   3) Write the three pointers into Footer fields, and write top_level_column_uids into Footer field
//   4) Clear footer.columns() to enable true on-demand meta (external-first, inline fallback)
//
// Note: This header centralizes keys, parsing/validation and read/write helpers to avoid duplication.

// No file_meta_datas keys are used for CMO pointers or uid mapping anymore.

// Wrapper class to expose static methods for external column meta utilities.
class ExternalColMetaUtil {
public:
    struct ExternalMetaPointers {
        uint64_t region_start = 0;
        uint64_t cmo_offset = 0;
        uint32_t num_columns = 0;
    };

    // Parse three external meta pointers from Footer.file_meta_datas.
    static bool parse_external_meta_pointers(const SegmentFooterPB& footer,
                                             ExternalMetaPointers* out);

    // Parse uid->col_id mapping from v2 blob (raw i32 uid array of length = num_columns).
    // Returns true on success.
    static bool parse_uid_to_colid_map(const SegmentFooterPB& footer,
                                       const ExternalMetaPointers& ptrs,
                                       std::unordered_map<int32_t, size_t>* out);

    static bool is_valid_meta_slice(uint64_t pos, uint64_t size, const ExternalMetaPointers& p);

    // Read one ColumnMetaPB via CMO; use INDEX cache lane because it's small metadata.
    static Status read_col_meta_from_cmo(const io::FileReaderSPtr& file_reader,
                                         const ExternalMetaPointers& p, uint32_t col_id,
                                         ColumnMetaPB* out_meta);

    // Write external ColumnMetaPB contiguous region + CMO + footer pointers + uid2colid.
    // write_cb: a callback that appends slices to the underlying file (e.g., SegmentWriter::_write_raw_data).
    static Status write_external_column_meta(
            bool enabled, io::FileWriter* file_writer, SegmentFooterPB* footer,
            CompressionTypePB compression_type,
            const std::function<Status(const std::vector<Slice>&)>& write_cb);
};

} // namespace doris::segment_v2
