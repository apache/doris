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

#include <memory>
#include <string>
#include <unordered_map>

#include "gen_cpp/segment_v2.pb.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "olap/rowset/segment_v2/indexed_column_writer.h"
#include "olap/types.h"
#include "util/slice.h"
#include "vec/json/path_in_data.h"

namespace doris::segment_v2 {

// -----------------------------------------------------------------------------
// Variant External Meta (path -> ColumnMetaPB bytes) storage format
// -----------------------------------------------------------------------------
// Purpose: externalize ColumnMetaPB of variant subcolumns as a mapping from
// relative path to serialized ColumnMetaPB bytes, instead of packing all into
// Footer.columns. This enables on-demand subcolumn loading and fast path lookup.
//
// Physical layout (per variant root uid): two IndexedColumns are written
//   - variant_meta_keys.<root_uid>    : IndexedColumn(VARCHAR)
//       • value index: ON   (fast existence/lookup by path)
//       • ordinal index: OFF
//       • encoding: PREFIX_ENCODING
//   - variant_meta_values.<root_uid>  : IndexedColumn(VARCHAR)
//       • value index: OFF
//       • ordinal index: ON (same write order as keys, 1:1 position mapping)
//       • encoding: PLAIN_ENCODING
//
// Footer carrier:
//   footer.file_meta_datas contains two entries per root uid:
//     - key = "variant_meta_keys.<root_uid>"    , value = IndexedColumnMetaPB bytes
//     - key = "variant_meta_values.<root_uid>"  , value = IndexedColumnMetaPB bytes
//
// Write workflow (this class):
//   1) externalize_from_footer() scans Footer.columns:
//      - keep root (relative path empty)
//      - collect sparse subcolumns (__DORIS_VARIANT_SPARSE__ including buckets) to embed into
//        the root variant's ColumnMetaPB.children_columns
//      - collect other materialized subcolumns as (relative_path, ColumnMetaPB bytes) to externalize
//   2) write (path, meta_bytes) in ascending path order into keys/values IndexedColumns
//   3) finish writers and persist their IndexedColumnMetaPB into footer.file_meta_datas
//   4) replace Footer.columns with only the kept top-level columns (roots included),
//      where roots already contain embedded sparse subcolumns; thus externalized non-sparse
//      subcolumns are pruned from footer.columns
//
// Read (brief): VariantExternalMetaReader uses keys' value index to locate the path,
// then reads values at the same ordinal to get ColumnMetaPB bytes and parses it.
//
// Interop with external column meta (column_meta_entries):
//   - Variant root ColumnMetaPB is externalized via column_meta_entries (addressed by col_id)
//   - Variant subcolumn ColumnMetaPB:
//       • non-sparse: externalized via this path-based meta (VariantExternalMetaReader)
//       • sparse (including buckets): embedded into root's ColumnMetaPB.children_columns and
//         loaded together with root meta via column_meta_entries

// Aggregate all externalized non-sparse subcolumn metas and flush them as two
// IndexedColumns' metas (keys/values) into footer.file_meta_datas, and prune these
// non-sparse subcolumns from footer.columns (sparse ones have been embedded into roots).
class VariantExtMetaWriter {
public:
    VariantExtMetaWriter(io::FileWriter* fw, CompressionTypePB comp) : _fw(fw), _comp(comp) {}

    // Add one path->meta mapping for the specified variant root uid.
    // key: subcolumn path (VARCHAR)
    // val: serialized ColumnMetaPB (VARCHAR)
    Status add(int32_t root_uid, const Slice& key, const Slice& val);

    // Finish writers and append their metas into footer.file_meta_datas.
    Status flush_to_footer(SegmentFooterPB* footer);

    // Scan footer.columns, find variant extracted subcolumns, externalize them into
    // ext meta index and remove them from footer.columns. This method both writes
    // keys/values metas to footer.file_meta_datas and prunes subcolumns from footer.
    Status externalize_from_footer(SegmentFooterPB* footer);

    bool empty() const { return _writers_by_uid.empty(); }

private:
    struct Writers {
        std::unique_ptr<IndexedColumnWriter> key_writer; // value index
        std::unique_ptr<IndexedColumnWriter> val_writer; // ordinal index
        size_t count = 0;
        bool inited = false;
    };

    Status _ensure_inited(Writers* w);

    io::FileWriter* _fw;
    CompressionTypePB _comp;
    std::unordered_map<int32_t, Writers> _writers_by_uid;
};

} // namespace doris::segment_v2
