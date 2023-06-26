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

#include "data_type_nullable_serde.h"

#include <arrow/array/array_base.h>
#include <gen_cpp/types.pb.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <memory>

#include "util/jsonb_document.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/data_types/serde/data_type_serde.h"

namespace doris {

namespace vectorized {
class Arena;

Status DataTypeNullableSerDe::write_column_to_pb(const IColumn& column, PValues& result, int start,
                                                 int end) const {
    int row_count = end - start;
    auto& nullable_col = assert_cast<const ColumnNullable&>(column);
    auto& null_col = nullable_col.get_null_map_column();
    if (nullable_col.has_null(row_count)) {
        result.set_has_null(true);
        auto* null_map = result.mutable_null_map();
        null_map->Reserve(row_count);
        const auto* col = check_and_get_column<ColumnUInt8>(null_col);
        auto& data = col->get_data();
        null_map->Add(data.begin() + start, data.begin() + end);
    }
    return nested_serde->write_column_to_pb(nullable_col.get_nested_column(), result, start, end);
}

// read from PValues to column
Status DataTypeNullableSerDe::read_column_from_pb(IColumn& column, const PValues& arg) const {
    auto& col = reinterpret_cast<ColumnNullable&>(column);
    auto& null_map_data = col.get_null_map_data();
    auto& nested = col.get_nested_column();
    if (Status st = nested_serde->read_column_from_pb(nested, arg); st != Status::OK()) {
        return st;
    }
    null_map_data.resize(nested.size());
    if (arg.has_null()) {
        for (int i = 0; i < arg.null_map_size(); ++i) {
            null_map_data[i] = arg.null_map(i);
        }
    } else {
        for (int i = 0; i < nested.size(); ++i) {
            null_map_data[i] = false;
        }
    }
    return Status::OK();
}
void DataTypeNullableSerDe::write_one_cell_to_jsonb(const IColumn& column, JsonbWriter& result,
                                                    Arena* mem_pool, int32_t col_id,
                                                    int row_num) const {
    auto& nullable_col = assert_cast<const ColumnNullable&>(column);
    if (nullable_col.is_null_at(row_num)) {
        // do not insert to jsonb
        return;
    }
    result.writeKey(col_id);
    nested_serde->write_one_cell_to_jsonb(nullable_col.get_nested_column(), result, mem_pool,
                                          col_id, row_num);
}
void DataTypeNullableSerDe::read_one_cell_from_jsonb(IColumn& column, const JsonbValue* arg) const {
    auto& col = reinterpret_cast<ColumnNullable&>(column);
    if (!arg || arg->isNull()) {
        col.insert_default();
        return;
    }
    nested_serde->read_one_cell_from_jsonb(col.get_nested_column(), arg);
    auto& null_map_data = col.get_null_map_data();
    null_map_data.push_back(0);
}

/**nullable serialize to arrow
   1/ convert the null_map from doris to arrow null byte map
   2/ pass the arrow null byteamp to nested column , and call AppendValues
**/
void DataTypeNullableSerDe::write_column_to_arrow(const IColumn& column, const UInt8* null_map,
                                                  arrow::ArrayBuilder* array_builder, int start,
                                                  int end) const {
    const auto& column_nullable = assert_cast<const ColumnNullable&>(column);
    const PaddedPODArray<UInt8>& bytemap = column_nullable.get_null_map_data();
    PaddedPODArray<UInt8> res;
    if (column_nullable.has_null()) {
        res.reserve(end - start);
        for (size_t i = start; i < end; ++i) {
            res.emplace_back(
                    !(bytemap)[i]); //Invert values since Arrow interprets 1 as a non-null value
        }
    }
    const UInt8* arrow_null_bytemap_raw_ptr = res.empty() ? nullptr : res.data();
    nested_serde->write_column_to_arrow(column_nullable.get_nested_column(),
                                        arrow_null_bytemap_raw_ptr, array_builder, start, end);
}

void DataTypeNullableSerDe::read_column_from_arrow(IColumn& column, const arrow::Array* arrow_array,
                                                   int start, int end,
                                                   const cctz::time_zone& ctz) const {
    auto& col = reinterpret_cast<ColumnNullable&>(column);
    NullMap& map_data = col.get_null_map_data();
    for (size_t i = start; i < end; ++i) {
        auto is_null = arrow_array->IsNull(i);
        map_data.emplace_back(is_null);
    }
    return nested_serde->read_column_from_arrow(col.get_nested_column(), arrow_array, start, end,
                                                ctz);
}

template <bool is_binary_format>
Status DataTypeNullableSerDe::_write_column_to_mysql(const IColumn& column,
                                                     MysqlRowBuffer<is_binary_format>& result,
                                                     int row_idx, bool col_const) const {
    auto& col = static_cast<const ColumnNullable&>(column);
    auto& nested_col = col.get_nested_column();
    const auto col_index = index_check_const(row_idx, col_const);
    if (col.has_null() && col.is_null_at(col_index)) {
        if (UNLIKELY(0 != result.push_null())) {
            return Status::InternalError("pack mysql buffer failed.");
        }
    } else {
        RETURN_IF_ERROR(
                nested_serde->write_column_to_mysql(nested_col, result, col_index, col_const));
    }
    return Status::OK();
}

Status DataTypeNullableSerDe::write_column_to_mysql(const IColumn& column,
                                                    MysqlRowBuffer<true>& row_buffer, int row_idx,
                                                    bool col_const) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const);
}

Status DataTypeNullableSerDe::write_column_to_mysql(const IColumn& column,
                                                    MysqlRowBuffer<false>& row_buffer, int row_idx,
                                                    bool col_const) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const);
}

} // namespace vectorized
} // namespace doris
