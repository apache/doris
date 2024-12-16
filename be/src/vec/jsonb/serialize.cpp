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
#include <unordered_set>
#include <vector>

#include "olap/tablet_schema.h"
#include "runtime/descriptors.h"
#include "runtime/jsonb_value.h"
#include "runtime/primitive_type.h"
#include "runtime/types.h"
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
                                        const DataTypeSerDeSPtrs& serdes,
                                        const std::unordered_set<int32_t>& row_store_cids) {
    auto num_rows = block.rows();
    Arena pool;
    assert(num_cols <= block.columns());
    for (int i = 0; i < num_rows; ++i) {
        JsonbWriterT<JsonbOutStream> jsonb_writer;
        jsonb_writer.writeStartObject();
        for (int j = 0; j < num_cols; ++j) {
            const auto& column = block.get_by_position(j).column;
            const auto& tablet_column = *schema.columns()[j];
            // ignore row store columns
            if (tablet_column.is_row_store_column()) {
                continue;
            }
            // TODO improve performance for checking column in group
            if (row_store_cids.empty() || row_store_cids.contains(tablet_column.unique_id())) {
                serdes[j]->write_one_cell_to_jsonb(*column, jsonb_writer, &pool,
                                                   tablet_column.unique_id(), i);
            }
        }
        jsonb_writer.writeEndObject();
        dst.insert_data(jsonb_writer.getOutput()->getBuffer(), jsonb_writer.getOutput()->getSize());
    }
}

// batch rows
void JsonbSerializeUtil::jsonb_to_block(const DataTypeSerDeSPtrs& serdes,
                                        const ColumnString& jsonb_column,
                                        const std::unordered_map<uint32_t, uint32_t>& col_id_to_idx,
                                        Block& dst, const std::vector<std::string>& default_values,
                                        const std::unordered_set<int>& include_cids) {
    for (int i = 0; i < jsonb_column.size(); ++i) {
        StringRef jsonb_data = jsonb_column.get_data_at(i);
        jsonb_to_block(serdes, jsonb_data.data, jsonb_data.size, col_id_to_idx, dst, default_values,
                       include_cids);
    }
}

// single row
void JsonbSerializeUtil::jsonb_to_block(const DataTypeSerDeSPtrs& serdes, const char* data,
                                        size_t size,
                                        const std::unordered_map<uint32_t, uint32_t>& col_id_to_idx,
                                        Block& dst, const std::vector<std::string>& default_values,
                                        const std::unordered_set<int>& include_cids) {
    auto pdoc = JsonbDocument::createDocument(data, size);
    JsonbDocument& doc = *pdoc;
    size_t num_rows = dst.rows();
    size_t filled_columns = 0;
    for (auto it = doc->begin(); it != doc->end(); ++it) {
        auto col_it = col_id_to_idx.find(it->getKeyId());
        if (col_it != col_id_to_idx.end() &&
            (include_cids.empty() || include_cids.contains(it->getKeyId()))) {
            MutableColumnPtr dst_column =
                    dst.get_by_position(col_it->second).column->assume_mutable();
            serdes[col_it->second]->read_one_cell_from_jsonb(*dst_column, it->value());
            ++filled_columns;
        }
    }
    if (filled_columns >= dst.columns()) {
        return;
    }
    auto fill_column = [&](Block& dst, int pos, size_t old_num_rows) {
        MutableColumnPtr dst_column = dst.get_by_position(pos).column->assume_mutable();
        if (dst_column->size() < old_num_rows + 1) {
            DCHECK(dst_column->size() == old_num_rows);
            if (default_values[pos].empty()) {
                dst_column->insert_default();
            } else {
                Slice value(default_values[pos].data(), default_values[pos].size());
                DataTypeSerDe::FormatOptions opt;
                opt.converted_from_string = true;
                static_cast<void>(
                        serdes[pos]->deserialize_one_cell_from_json(*dst_column, value, opt));
            }
        }
        DCHECK(dst_column->size() == num_rows + 1);
    };
    // fill missing column
    if (!include_cids.empty()) {
        for (auto cid : include_cids) {
            auto col_it = col_id_to_idx.find(cid);
            if (col_it == col_id_to_idx.end()) {
                continue;
            }
            fill_column(dst, col_it->second, num_rows);
        }
    } else {
        for (int i = 0; i < dst.columns(); ++i) {
            fill_column(dst, i, num_rows);
        }
    }
}

} // namespace doris::vectorized