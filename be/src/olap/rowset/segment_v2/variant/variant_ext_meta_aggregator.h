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

// Aggregates externalized meta for Variant subcolumns and flushes
// them as two IndexedColumns into footer.file_meta_datas:
// - variant_meta_keys.<root_uid>
// - variant_meta_values.<root_uid>
class VariantExtMetaAggregator {
public:
    VariantExtMetaAggregator(io::FileWriter* fw, CompressionTypePB comp) : _fw(fw), _comp(comp) {}

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
