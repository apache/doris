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
#include "util/jsonb_document.h"
#include "util/jsonb_stream.h"
#include "util/jsonb_writer.h"
#include "vec/columns/column.h"
#include "vec/columns/column_string.h"
#include "vec/common/arena.h"
#include "vec/common/string_ref.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/serde/data_type_serde.h"

namespace doris::vectorized {

void JsonbSerializeUtil::block_to_jsonb(const TabletSchema& schema, const Block& block,
                                        ColumnString& dst, int num_cols) {
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
            block.get_data_type(j)->get_serde()->write_one_cell_to_jsonb(
                    *column, jsonb_writer, &pool, tablet_column.unique_id(), i);
        }
        jsonb_writer.writeEndObject();
        dst.insert_data(jsonb_writer.getOutput()->getBuffer(), jsonb_writer.getOutput()->getSize());
    }
}

// batch rows
void JsonbSerializeUtil::jsonb_to_block(const TupleDescriptor& desc,
                                        const ColumnString& jsonb_column, Block& dst) {
    for (int i = 0; i < jsonb_column.size(); ++i) {
        StringRef jsonb_data = jsonb_column.get_data_at(i);
        jsonb_to_block(desc, jsonb_data.data, jsonb_data.size, dst);
    }
}

// single row
void JsonbSerializeUtil::jsonb_to_block(const TupleDescriptor& desc, const char* data, size_t size,
                                        Block& dst) {
    auto pdoc = JsonbDocument::createDocument(data, size);
    JsonbDocument& doc = *pdoc;
    for (int j = 0; j < desc.slots().size(); ++j) {
        SlotDescriptor* slot = desc.slots()[j];
        JsonbValue* slot_value = doc->find(slot->col_unique_id());
        MutableColumnPtr dst_column = dst.get_by_position(j).column->assume_mutable();
        if (!slot_value || slot_value->isNull()) {
            // null or not exist
            dst_column->insert_default();
            continue;
        }
        dst.get_data_type(j)->get_serde()->read_one_cell_from_jsonb(*dst_column, slot_value);
    }
}

void JsonbSerializeUtil::jsonb_to_block(TabletSchemaSPtr schema,
                                        const std::vector<uint32_t>& col_ids,
                                        const ColumnString& jsonb_column, Block& dst) {
    for (int i = 0; i < jsonb_column.size(); ++i) {
        StringRef jsonb_data = jsonb_column.get_data_at(i);
        jsonb_to_block(schema, col_ids, jsonb_data.data, jsonb_data.size, dst);
    }
}

void JsonbSerializeUtil::jsonb_to_block(TabletSchemaSPtr schema,
                                        const std::vector<uint32_t>& col_ids, const char* data,
                                        size_t size, Block& dst) {
    auto pdoc = JsonbDocument::createDocument(data, size);
    JsonbDocument& doc = *pdoc;
    for (int j = 0; j < col_ids.size(); ++j) {
        auto column = schema->column(col_ids[j]);
        JsonbValue* slot_value = doc->find(column.unique_id());
        MutableColumnPtr dst_column = dst.get_by_position(j).column->assume_mutable();
        if (!slot_value || slot_value->isNull()) {
            // null or not exist
            dst_column->insert_default();
            continue;
        }
        dst.get_data_type(j)->get_serde()->read_one_cell_from_jsonb(*dst_column, slot_value);
    }
}

} // namespace doris::vectorized