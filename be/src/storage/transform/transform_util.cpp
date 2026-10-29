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
#include "storage/tablet/tablet_schema.h"

namespace doris::segment_v2 {

void collect_key_columns(const TabletSchema& schema, const Block& block,
                         std::vector<const IColumn*>& key_columns) {
    key_columns.clear();
    const uint32_t num_key_columns = cast_set<uint32_t>(schema.num_key_columns());
    for (uint32_t cid = 0; cid < num_key_columns; ++cid) {
        key_columns.push_back(block.get_by_position(cid).column.get());
    }
}

Block widen_partial_update_block(const TabletSchema& schema,
                                 const std::vector<uint32_t>& update_cids, const Block& narrow) {
    Block full_block = schema.create_storage_block();
    size_t input_id = 0;
    for (auto cid : update_cids) {
        // Carry the input's type along with its column: a variant V2 input column
        // is typed differently from the slot the schema-created block holds.
        const auto& input_column = narrow.get_by_position(input_id++);
        auto& full_column = full_block.get_by_position(cid);
        full_column.column = input_column.column;
        full_column.type = input_column.type;
    }
    return full_block;
}

} // namespace doris::segment_v2
