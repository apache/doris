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

#include "storage/transform/transform_util.h"

#include "common/cast_set.h"
#include "core/block/block.h"
#include "storage/iterator/olap_data_convertor.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::segment_v2 {

Status convert_key_columns(OlapBlockDataConvertor& convertor, const TabletSchema& schema,
                           const Block& block, size_t num_rows,
                           std::vector<IOlapColumnDataAccessor*>& key_columns) {
    key_columns.clear();
    const uint32_t num_key_columns = cast_set<uint32_t>(schema.num_key_columns());
    for (uint32_t cid = 0; cid < num_key_columns; ++cid) {
        convertor.add_column_data_convertor_at(schema.column(cid), cid);
        RETURN_IF_ERROR(convertor.set_source_content_with_specifid_column(
                block.get_by_position(cid), 0, num_rows, cid));
        auto [st, column] = convertor.convert_column_data(cid);
        RETURN_IF_ERROR(st);
        key_columns.push_back(column);
    }
    return Status::OK();
}

Status convert_seq_column(OlapBlockDataConvertor& convertor, const TabletSchema& schema,
                          const Block& block, size_t src_pos, size_t num_rows,
                          IOlapColumnDataAccessor*& seq_column) {
    const auto seq_cid = cast_set<uint32_t>(schema.sequence_col_idx());
    convertor.add_column_data_convertor_at(schema.column(seq_cid), seq_cid);
    RETURN_IF_ERROR(convertor.set_source_content_with_specifid_column(
            block.get_by_position(src_pos), 0, num_rows, seq_cid));
    auto [st, column] = convertor.convert_column_data(seq_cid);
    RETURN_IF_ERROR(st);
    seq_column = column;
    return Status::OK();
}

Block widen_partial_update_block(const TabletSchema& schema,
                                 const std::vector<uint32_t>& update_cids, const Block& narrow) {
    Block full_block = schema.create_block();
    size_t input_id = 0;
    for (auto cid : update_cids) {
        full_block.replace_by_position(cid, narrow.get_by_position(input_id++).column);
    }
    return full_block;
}

} // namespace doris::segment_v2
