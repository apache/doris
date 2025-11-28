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
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "gen_cpp/segment_v2.pb.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "olap/rowset/segment_v2/indexed_column_writer.h"
#include "olap/types.h"
#include "util/slice.h"

namespace doris::segment_v2 {

// -----------------------------------------------------------------------------
// Variant External Meta (path -> ColumnMetaPB bytes) storage format
// -----------------------------------------------------------------------------
// Purpose: externalize ColumnMetaPB of variant subcolumns as a mapping from
// relative path to column meta entries in the external Column Meta Region,
// instead of packing all into Footer.columns. This enables on-demand subcolumn
// loading and fast path lookup.
//
// Physical layout (per variant root uid): one IndexedColumn of keys is written
//   - variant_meta_keys.<root_uid>    : IndexedColumn(VARCHAR)
//       • value index: ON   (fast existence/lookup by path)
//       • ordinal index: ON (enable full scan / load_all)
//       • encoding: PREFIX_ENCODING
//       • content: Key = Path (relative path string only)
//
// Footer carrier:
//   footer.file_meta_datas contains one entry per root uid:
//     - key = "variant_meta_keys.<root_uid>"    , value = IndexedColumnMetaPB bytes
//
// Interop with external column meta (column_meta_entries):
//   - Variant root ColumnMetaPB is externalized via column_meta_entries (addressed by col_id)
//   - Variant subcolumn ColumnMetaPB:
//       • non-sparse: stored in Column Meta Region, addressed by a unique col_id (in column_meta_entries).
//         The mapping from Path -> col_id is stored in this class (VariantExtMetaWriter).
//       • sparse (including buckets): embedded into root's ColumnMetaPB.children_columns and
//         loaded together with root meta via column_meta_entries
//
class VariantExtMetaWriter {
public:
    VariantExtMetaWriter(io::FileWriter* fw, CompressionTypePB comp) : _fw(fw), _comp(comp) {}

    // Add one path entry for the specified variant root uid.
    // key: subcolumn path (VARCHAR)
    // The concrete ColumnMetaPB location (column_id) is derived on read side
    // via (root_column_id + 1 + ordinal) based on the agreed Column Meta Region
    // layout.
    Status add(int32_t root_uid, const Slice& key);

    // Finish writers and append their metas into footer.file_meta_datas.
    Status flush_to_footer(SegmentFooterPB* footer);

    // Scan footer.columns, find variant extracted subcolumns, externalize them into
    // ext meta index and remove them from footer.columns.
    // This method reorganizes the columns:
    // 1. Separates Top Level Columns (kept in footer) and Variant Subcolumns.
    // 2. Records Subcolumns' indices (relative to the combined list) into the Variant Index.
    // 3. Populates out_metas with the combined list of all columns (Top Level + Subcolumns),
    //    ordered such that Subcolumns immediately follow their Root Variant.
    Status externalize_from_footer(SegmentFooterPB* footer, std::vector<ColumnMetaPB>* out_metas);

    bool empty() const { return _writers_by_uid.empty(); }

private:
    struct Writers {
        std::unique_ptr<IndexedColumnWriter> key_writer; // value index
        size_t count = 0;
        bool inited = false;
    };

    Status _ensure_inited(Writers* w);

    io::FileWriter* _fw;
    CompressionTypePB _comp;
    std::unordered_map<int32_t, Writers> _writers_by_uid;
};

} // namespace doris::segment_v2
