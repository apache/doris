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

#include "vec/jsonb/serialize.h"

#include <assert.h>

#include <algorithm>
#include <memory>
#include <vector>

#include "olap/tablet_schema.h"
#include "runtime/descriptors.h"
#include "runtime/jsonb_value.h"
#include "runtime/primitive_type.h"
#include "runtime/types.h"
#include "util/bitmap_value.h"
#include "util/jsonb_document.h"
#include "util/jsonb_stream.h"
#include "util/jsonb_writer.h"
#include "vec/columns/column.h"
#include "vec/columns/column_string.h"
#include "vec/common/arena.h"
#include "vec/common/string_ref.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/serde/data_type_serde.h"
#include "vec/io/reader_buffer.h"

namespace doris::vectorized {

void JsonbSerializeUtil::block_to_jsonb(const TabletSchema& schema, const Block& block,
                                        ColumnString& dst, int num_cols,
                                        const DataTypeSerDeSPtrs& serdes) {
    auto num_rows = block.rows();
    Arena pool;
    assert(num_cols <= block.columns());
    for (int i = 0; i < num_rows; ++i) {
        JsonbWriterT<JsonbOutStream> jsonb_writer;
        jsonb_writer.writeStartObject();
        for (int j = 0; j < num_cols; ++j) {
            const auto& column = block.get_by_position(j).column;
            const auto& tablet_column = schema.columns()[j];
            if (tablet_column.is_row_store_column()) {
                // ignore dst row store column
                continue;
            }
            serdes[j]->write_one_cell_to_jsonb(*column, jsonb_writer, &pool,
                                               tablet_column.unique_id(), i);
        }
        jsonb_writer.writeEndObject();
        dst.insert_data(jsonb_writer.getOutput()->getBuffer(), jsonb_writer.getOutput()->getSize());
    }
}

// batch rows
void JsonbSerializeUtil::jsonb_to_block(const DataTypeSerDeSPtrs& serdes,
                                        const ColumnString& jsonb_column,
                                        const std::unordered_map<uint32_t, uint32_t>& col_id_to_idx,
                                        Block& dst,
                                        const std::vector<std::string>& default_values) {
    for (int i = 0; i < jsonb_column.size(); ++i) {
        StringRef jsonb_data = jsonb_column.get_data_at(i);
        jsonb_to_block(serdes, jsonb_data.data, jsonb_data.size, col_id_to_idx, dst,
                       default_values);
    }
}

void JsonbSerializeUtil::jsonb_to_block(
        const DataTypeSerDeSPtrs& serdes_full_read, const DataTypeSerDeSPtrs& serdes_point_read,
        const ColumnString& jsonb_column,
        const std::unordered_map<uint32_t, uint32_t>& col_uid_to_idx_full_read,
        const std::unordered_map<uint32_t, std::pair<uint32_t, uint32_t>>&
                col_uid_to_idx_cid_point_read,
        const std::vector<ReadRowsInfo>& rows_info, Block& block_full_read,
        Block& block_point_read) {
    DCHECK(jsonb_column.size() == rows_info.size());
    for (int i = 0; i < jsonb_column.size(); ++i) {
        StringRef jsonb_data = jsonb_column.get_data_at(i);
        std::unordered_map<uint32_t, bool> point_read_cids;
        for (uint32_t cid : rows_info[i].cids) {
            point_read_cids.emplace(cid, false);
        }
        jsonb_to_block(serdes_full_read, serdes_point_read, jsonb_data.data, jsonb_data.size,
                       col_uid_to_idx_full_read, col_uid_to_idx_cid_point_read, point_read_cids,
                       block_full_read, block_point_read);
    }
}

// single row
void JsonbSerializeUtil::jsonb_to_block(const DataTypeSerDeSPtrs& serdes, const char* data,
                                        size_t size,
                                        const std::unordered_map<uint32_t, uint32_t>& col_id_to_idx,
                                        Block& dst,
                                        const std::vector<std::string>& default_values) {
    auto pdoc = JsonbDocument::createDocument(data, size);
    JsonbDocument& doc = *pdoc;
    size_t num_rows = dst.rows();
    size_t filled_columns = 0;
    for (auto it = doc->begin(); it != doc->end(); ++it) {
        auto col_it = col_id_to_idx.find(it->getKeyId());
        if (col_it != col_id_to_idx.end()) {
            MutableColumnPtr dst_column =
                    dst.get_by_position(col_it->second).column->assume_mutable();
            serdes[col_it->second]->read_one_cell_from_jsonb(*dst_column, it->value());
            ++filled_columns;
        }
    }
    if (filled_columns < dst.columns()) {
        // fill missing slot
        for (int i = 0; i < dst.columns(); ++i) {
            const auto& column_type_name = dst.get_by_position(i);
            MutableColumnPtr col = column_type_name.column->assume_mutable();
            if (col->size() < num_rows + 1) {
                DCHECK(col->size() == num_rows);
                if (default_values[i].empty()) {
                    col->insert_default();
                } else {
                    Slice value(default_values[i].data(), default_values[i].size());
                    DataTypeSerDe::FormatOptions opt;
                    opt.converted_from_string = true;
                    static_cast<void>(serdes[i]->deserialize_one_cell_from_json(*col, value, opt));
                }
            }
            DCHECK(col->size() == num_rows + 1);
        }
    }
}

void JsonbSerializeUtil::jsonb_to_block(
        const DataTypeSerDeSPtrs& serdes_full_read, const DataTypeSerDeSPtrs& serdes_point_read,
        const char* data, size_t size,
        const std::unordered_map<uint32_t, uint32_t>& col_uid_to_idx_full_read,
        const std::unordered_map<uint32_t, std::pair<uint32_t, uint32_t>>&
                col_uid_to_idx_cid_point_read,
        std::unordered_map<uint32_t, bool>& point_read_cids, Block& block_full_read,
        Block& block_point_read) {
    JsonbDocument& doc = *JsonbDocument::createDocument(data, size);
    size_t full_read_filled_columns = 0;
    for (auto it = doc->begin(); it != doc->end(); ++it) {
        auto col_it = col_uid_to_idx_full_read.find(it->getKeyId());
        if (col_it != col_uid_to_idx_full_read.end()) {
            MutableColumnPtr dst_column =
                    block_full_read.get_by_position(col_it->second).column->assume_mutable();
            serdes_full_read[col_it->second]->read_one_cell_from_jsonb(*dst_column, it->value());
            ++full_read_filled_columns;
        } else {
            auto it2 = col_uid_to_idx_cid_point_read.find(it->getKeyId());
            if (it2 != col_uid_to_idx_cid_point_read.end()) {
                uint32_t cid = it2->second.second;
                uint32_t idx = it2->second.first;
                auto it3 = point_read_cids.find(cid);
                if (it3 != point_read_cids.end()) {
                    MutableColumnPtr dst_column =
                            block_point_read.get_by_position(idx).column->assume_mutable();
                    serdes_point_read[idx]->read_one_cell_from_jsonb(*dst_column, it->value());
                }
            }
        }
    }
    // __DORIS_ROW_STORE_COL__ column
    CHECK(full_read_filled_columns + 1 == block_full_read.columns())
            << "full_read_filled_columns=" << full_read_filled_columns
            << ", block_full_read.columns():" << block_full_read.columns();
}

} // namespace doris::vectorized