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

#include "util/jsonb/serialize.h"

#include <assert.h>

#include <algorithm>
#include <memory>
#include <unordered_set>
#include <vector>

#include "core/arena.h"
#include "core/block/column_with_type_and_name.h"
#include "core/block/columns_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type.h"
#include "core/data_type/primitive_type.h"
#include "core/data_type_serde/data_type_serde.h"
#include "core/string_ref.h"
#include "core/value/jsonb_value.h"
#include "runtime/descriptors.h"
#include "storage/tablet/tablet_schema.h"
#include "util/defer_op.h"
#include "util/jsonb_document.h"
#include "util/jsonb_stream.h"
#include "util/jsonb_writer.h"

namespace doris {

void JsonbSerializeUtil::block_to_jsonb(const TabletSchema& schema, const Block& block,
                                        ColumnString& dst, int num_cols,
                                        const DataTypeSerDeSPtrs& serdes,
                                        const std::unordered_set<int32_t>& row_store_cids) {
    auto num_rows = block.rows();
    Arena arena;
    assert(num_cols <= block.columns());
    DataTypeSerDe::FormatOptions options;
    auto tz = cctz::utc_time_zone();
    options.timezone = &tz;
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
                serdes[j]->write_one_cell_to_jsonb(*column, jsonb_writer, arena,
                                                   tablet_column.unique_id(), i, options);
            }
        }
        jsonb_writer.writeEndObject();
        dst.insert_data(jsonb_writer.getOutput()->getBuffer(), jsonb_writer.getOutput()->getSize());
    }
}

// batch rows
Status JsonbSerializeUtil::jsonb_to_block(
        const DataTypeSerDeSPtrs& serdes, const ColumnString& jsonb_column,
        const std::unordered_map<uint32_t, uint32_t>& col_id_to_idx, Block& dst,
        const std::vector<std::string>& default_values,
        const std::unordered_set<int>& include_cids) {
    auto dst_columns_guard = dst.mutate_columns_scoped();
    MutableColumns& dst_columns = dst_columns_guard.mutable_columns();
    for (int i = 0; i < jsonb_column.size(); ++i) {
        StringRef jsonb_data = jsonb_column.get_data_at(i);
        RETURN_IF_ERROR(jsonb_to_columns(serdes, jsonb_data.data, jsonb_data.size, col_id_to_idx,
                                         dst_columns, default_values, include_cids));
    }
    return Status::OK();
}

Status JsonbSerializeUtil::jsonb_to_columns(
        const DataTypeSerDeSPtrs& serdes, const char* data, size_t size,
        const std::unordered_map<uint32_t, uint32_t>& col_id_to_idx, MutableColumns& dst_columns,
        const std::vector<std::string>& default_values,
        const std::unordered_set<int>& include_cids) {
    const JsonbDocument* pdoc = nullptr;
    RETURN_IF_ERROR(JsonbDocument::checkAndCreateDocument(data, size, &pdoc));
    const JsonbDocument& doc = *pdoc;
    DCHECK(!dst_columns.empty());
    size_t num_rows = dst_columns[0]->size();
    size_t filled_columns = 0;
    for (auto it = doc->begin(); it != doc->end(); ++it) {
        auto col_it = col_id_to_idx.find(it->getKeyId());
        if (col_it != col_id_to_idx.end() &&
            (include_cids.empty() || include_cids.contains(it->getKeyId()))) {
            auto& dst_column = dst_columns[col_it->second];
            serdes[col_it->second]->read_one_cell_from_jsonb(*dst_column, it->value());
            ++filled_columns;
        }
    }
    if (filled_columns >= dst_columns.size()) {
        return Status::OK();
    }
    auto fill_column = [&](size_t pos, size_t old_num_rows) {
        auto& dst_column = dst_columns[pos];
        if (dst_column->size() < old_num_rows + 1) {
            DCHECK(dst_column->size() == old_num_rows);
            Status st = Status::OK();
            if (default_values[pos].empty()) {
                dst_column->insert_default();
            } else {
                Slice value(default_values[pos].data(), default_values[pos].size());
                DataTypeSerDe::FormatOptions opt;
                opt.converted_from_string = true;
                st = serdes[pos]->deserialize_one_cell_from_json(*dst_column, value, opt);
            }
            RETURN_IF_ERROR(st);
            DCHECK(dst_column->size() == num_rows + 1);
            return Status::OK();
        }
        DCHECK(dst_column->size() == num_rows + 1);
        return Status::OK();
    };
    // fill missing column
    if (!include_cids.empty()) {
        for (auto cid : include_cids) {
            auto col_it = col_id_to_idx.find(cid);
            if (col_it == col_id_to_idx.end()) {
                continue;
            }
            RETURN_IF_ERROR(fill_column(static_cast<size_t>(col_it->second), num_rows));
        }
    } else {
        for (size_t i = 0; i < dst_columns.size(); ++i) {
            RETURN_IF_ERROR(fill_column(i, num_rows));
        }
    }
    return Status::OK();
}

// single row
Status JsonbSerializeUtil::jsonb_to_block(
        const DataTypeSerDeSPtrs& serdes, const char* data, size_t size,
        const std::unordered_map<uint32_t, uint32_t>& col_id_to_idx, Block& dst,
        const std::vector<std::string>& default_values,
        const std::unordered_set<int>& include_cids) {
    auto dst_columns_guard = dst.mutate_columns_scoped();
    MutableColumns& dst_columns = dst_columns_guard.mutable_columns();
    return jsonb_to_columns(serdes, data, size, col_id_to_idx, dst_columns, default_values,
                            include_cids);
}

} // namespace doris
