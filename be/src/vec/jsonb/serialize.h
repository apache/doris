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
#include <stddef.h>

#include <unordered_map>

#include "olap/tablet_schema.h"
#include "runtime/descriptors.h"
#include "vec/columns/column_string.h"
#include "vec/core/block.h"

namespace doris {
class TabletSchema;
class TupleDescriptor;

namespace vectorized {
class ColumnString;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {
// use jsonb codec to store row format
class JsonbSerializeUtil {
public:
    static void block_to_jsonb(const TabletSchema& schema, const Block& block, ColumnString& dst,
                               int num_cols, const DataTypeSerDeSPtrs& serdes);
    // for batch rows
    static void jsonb_to_block(const DataTypeSerDeSPtrs& serdes, const ColumnString& jsonb_column,
                               const std::unordered_map<uint32_t, uint32_t>& col_id_to_idx,
                               Block& dst, const std::vector<std::string>& default_values);
    static void jsonb_to_block(
            const DataTypeSerDeSPtrs& serdes_full_read, const DataTypeSerDeSPtrs& serdes_point_read,
            const ColumnString& jsonb_column,
            const std::unordered_map<uint32_t, uint32_t>& col_uid_to_idx_full_read,
            const std::unordered_map<uint32_t, std::pair<uint32_t, uint32_t>>&
                    col_uid_to_idx_cid_point_read,
            const std::vector<ReadRowsInfo>& rows_info, Block& block_full_read,
            Block& block_point_read);

    // for a single row
    static void jsonb_to_block(const DataTypeSerDeSPtrs& serdes, const char* data, size_t size,
                               const std::unordered_map<uint32_t, uint32_t>& col_id_to_idx,
                               Block& dst, const std::vector<std::string>& default_values);
    static void jsonb_to_block(
            const DataTypeSerDeSPtrs& serdes_full_read, const DataTypeSerDeSPtrs& serdes_point_read,
            const char* data, size_t size,
            const std::unordered_map<uint32_t, uint32_t>& col_uid_to_idx_full_read,
            const std::unordered_map<uint32_t, std::pair<uint32_t, uint32_t>>&
                    col_uid_to_idx_cid_point_read,
            std::unordered_map<uint32_t, bool>& point_read_cids, Block& block_full_read,
            Block& block_point_read);
};
} // namespace doris::vectorized