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

#include "data_type_struct_serde.h"

#include "arrow/array/builder_nested.h"
#include "util/jsonb_document.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_struct.h"
#include "vec/common/string_ref.h"

namespace doris {

namespace vectorized {
class Arena;

void DataTypeStructSerDe::write_one_cell_to_jsonb(const IColumn& column, JsonbWriter& result,
                                                  Arena* mem_pool, int32_t col_id,
                                                  int row_num) const {
    result.writeKey(col_id);
    const char* begin = nullptr;
    // maybe serialize_value_into_arena should move to here later.
    StringRef value = column.serialize_value_into_arena(row_num, *mem_pool, begin);
    result.writeStartBinary();
    result.writeBinary(value.data, value.size);
    result.writeEndBinary();
}

void DataTypeStructSerDe::read_one_cell_from_jsonb(IColumn& column, const JsonbValue* arg) const {
    auto blob = static_cast<const JsonbBlobVal*>(arg);
    column.deserialize_and_insert_from_arena(blob->getBlob());
}

void DataTypeStructSerDe::write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                                arrow::ArrayBuilder* array_builder, int start,
                                                int end) const {
    auto& builder = assert_cast<arrow::StructBuilder&>(*array_builder);
    auto& struct_column = assert_cast<const ColumnStruct&>(column);
    for (int r = start; r < end; ++r) {
        if (null_map != nullptr && (*null_map)[r]) {
            checkArrowStatus(builder.AppendNull(), struct_column.get_name(),
                             builder.type()->name());
            continue;
        }
        checkArrowStatus(builder.Append(), struct_column.get_name(), builder.type()->name());
        for (size_t ei = 0; ei < struct_column.tuple_size(); ++ei) {
            auto elem_builder = builder.field_builder(ei);
            elemSerDeSPtrs[ei]->write_column_to_arrow(struct_column.get_column(ei), nullptr,
                                                      elem_builder, r, r + 1);
        }
    }
}

void DataTypeStructSerDe::read_column_from_arrow(IColumn& column, const arrow::Array* arrow_array,
                                                 int start, int end,
                                                 const cctz::time_zone& ctz) const {
    auto& struct_column = static_cast<ColumnStruct&>(column);
    auto concrete_struct = down_cast<const arrow::StructArray*>(arrow_array);
    DCHECK_EQ(struct_column.tuple_size(), concrete_struct->num_fields());
    for (size_t i = 0; i < struct_column.tuple_size(); ++i) {
        elemSerDeSPtrs[i]->read_column_from_arrow(struct_column.get_column(i),
                                                  concrete_struct->field(i).get(), start, end, ctz);
    }
}

template <bool is_binary_format>
Status DataTypeStructSerDe::_write_column_to_mysql(const IColumn& column,
                                                   MysqlRowBuffer<is_binary_format>& result,
                                                   int row_idx, bool col_const) const {
    auto& col = assert_cast<const ColumnStruct&>(column);
    const auto col_index = index_check_const(row_idx, col_const);
    result.open_dynamic_mode();
    if (0 != result.push_string("{", 1)) {
        return Status::InternalError("pack mysql buffer failed.");
    }
    bool begin = true;
    for (size_t j = 0; j < elemSerDeSPtrs.size(); ++j) {
        if (!begin) {
            if (0 != result.push_string(", ", 2)) {
                return Status::InternalError("pack mysql buffer failed.");
            }
        }

        if (col.get_column_ptr(j)->is_null_at(col_index)) {
            if (0 != result.push_string("NULL", strlen("NULL"))) {
                return Status::InternalError("pack mysql buffer failed.");
            }
        } else {
            if (remove_nullable(col.get_column_ptr(j))->is_column_string()) {
                if (0 != result.push_string("\"", 1)) {
                    return Status::InternalError("pack mysql buffer failed.");
                }
                RETURN_IF_ERROR(elemSerDeSPtrs[j]->write_column_to_mysql(col.get_column(j), result,
                                                                         col_index, col_const));
                if (0 != result.push_string("\"", 1)) {
                    return Status::InternalError("pack mysql buffer failed.");
                }
            } else {
                RETURN_IF_ERROR(elemSerDeSPtrs[j]->write_column_to_mysql(col.get_column(j), result,
                                                                         col_index, col_const));
            }
        }
        begin = false;
    }
    if (UNLIKELY(0 != result.push_string("}", 1))) {
        return Status::InternalError("pack mysql buffer failed.");
    }
    result.close_dynamic_mode();
    return Status::OK();
}

Status DataTypeStructSerDe::write_column_to_mysql(const IColumn& column,
                                                  MysqlRowBuffer<true>& row_buffer, int row_idx,
                                                  bool col_const) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const);
}

Status DataTypeStructSerDe::write_column_to_mysql(const IColumn& column,
                                                  MysqlRowBuffer<false>& row_buffer, int row_idx,
                                                  bool col_const) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const);
}

} // namespace vectorized
} // namespace doris