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

#include "data_type_string_serde.h"
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

void DataTypeNullableSerDe::serialize_column_to_json(const IColumn& column, int start_idx,
                                                     int end_idx, BufferWritable& bw,
                                                     FormatOptions& options) const {
    SERIALIZE_COLUMN_TO_JSON()
}

void DataTypeNullableSerDe::serialize_one_cell_to_json(const IColumn& column, int row_num,
                                                       BufferWritable& bw,
                                                       FormatOptions& options) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    const auto& col_null = assert_cast<const ColumnNullable&>(*ptr);
    if (col_null.is_null_at(row_num)) {
        bw.write("NULL", 4);
    } else {
        nested_serde->serialize_one_cell_to_json(col_null.get_nested_column(), row_num, bw,
                                                 options);
    }
}

Status DataTypeNullableSerDe::deserialize_column_from_json_vector(IColumn& column,
                                                                  std::vector<Slice>& slices,
                                                                  int* num_deserialized,
                                                                  const FormatOptions& options,
                                                                  int nesting_level) const {
    DESERIALIZE_COLUMN_FROM_JSON_VECTOR();
    return Status::OK();
}

void DataTypeNullableSerDe::serialize_one_cell_to_hive_text(const IColumn& column, int row_num,
                                                            BufferWritable& bw,
                                                            FormatOptions& options,
                                                            int nesting_level) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    const auto& col_null = assert_cast<const ColumnNullable&>(*ptr);
    if (col_null.is_null_at(row_num)) {
        bw.write("\\N", 2);
    } else {
        nested_serde->serialize_one_cell_to_hive_text(col_null.get_nested_column(), row_num, bw,
                                                      options, nesting_level);
    }
}

Status DataTypeNullableSerDe::deserialize_one_cell_from_hive_text(IColumn& column, Slice& slice,
                                                                  const FormatOptions& options,
                                                                  int nesting_level) const {
    auto& null_column = assert_cast<ColumnNullable&>(column);
    if (slice.size == 2 && slice[0] == '\\' && slice[1] == 'N') {
        null_column.insert_data(nullptr, 0);
        return Status::OK();
    }

    Status st = nested_serde->deserialize_one_cell_from_hive_text(null_column.get_nested_column(),
                                                                  slice, options, nesting_level);

    if (!st.ok()) {
        // fill null if fail
        null_column.insert_data(nullptr, 0); // 0 is meaningless here
        return Status::OK();
    }

    // fill not null if success
    null_column.get_null_map_data().push_back(0);
    return Status::OK();
}

Status DataTypeNullableSerDe::deserialize_column_from_hive_text_vector(IColumn& column,
                                                                       std::vector<Slice>& slices,
                                                                       int* num_deserialized,
                                                                       const FormatOptions& options,
                                                                       int nesting_level) const {
    DESERIALIZE_COLUMN_FROM_HIVE_TEXT_VECTOR();
    return Status::OK();
}

Status DataTypeNullableSerDe::deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                                             const FormatOptions& options,
                                                             int nesting_level) const {
    auto& null_column = assert_cast<ColumnNullable&>(column);
    // TODO(Amory) make null literal configurable

    // only slice trim quote return true make sure slice is quoted and converted_from_string make
    // sure slice is from string parse , we can parse this "null" literal as string "null" to
    // nested column , otherwise we insert null to null column
    if (!(options.converted_from_string && slice.trim_quote())) {
        /*
         * For null values in ordinary types, we use \N to represent them;
         * for null values in nested types, we use null to represent them, just like the json format.
         *
         * example:
         * If you have three nullable columns
         *    a : int, b : string, c : map<string,int>
         * data:
         *      \N,hello world,\N
         *      1,\N,{"cmake":2,"null":11}
         *      9,"\N",{"\N":null,null:0}
         *      \N,"null",{null:null}
         *      null,null,null
         *
         * if you set trim_double_quotes = true
         * you will get :
         *      NULL,hello world,NULL
         *      1,NULL,{"cmake":2,"null":11}
         *      9,\N,{"\N":NULL,NULL:0}
         *      NULL,null,{NULL:NULL}
         *      NULL,null,NULL
         *
         * if you set trim_double_quotes = false
         * you will get :
         *      NULL,hello world,NULL
         *      1,\N,{"cmake":2,"null":11}
         *      9,"\N",{"\N":NULL,NULL:0}
         *      NULL,"null",{NULL:NULL}
         *      NULL,null,NULL
         *
         * in csv(text) for normal type: we only recognize \N for null , so
         * for not char family type, like int, if we put null literal ,
         * it will parse fail, and make result null，not just because it equals \N.
         * for char family type, like string, if we put null literal, it will parse success,
         * and "null" literal will be stored in doris.
         *
         */
        if (nesting_level >= 2 && slice.size == 4 && slice[0] == 'n' && slice[1] == 'u' &&
            slice[2] == 'l' && slice[3] == 'l') {
            null_column.insert_data(nullptr, 0);
            return Status::OK();
        } else if (nesting_level == 1 && slice.size == 2 && slice[0] == '\\' && slice[1] == 'N') {
            null_column.insert_data(nullptr, 0);
            return Status::OK();
        }
    }

    auto st = nested_serde->deserialize_one_cell_from_json(null_column.get_nested_column(), slice,
                                                           options, nesting_level);
    if (!st.ok()) {
        // fill null if fail
        null_column.insert_data(nullptr, 0); // 0 is meaningless here
        return Status::OK();
    }
    // fill not null if success
    null_column.get_null_map_data().push_back(0);
    return Status::OK();
}

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
void DataTypeNullableSerDe::write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                                  arrow::ArrayBuilder* array_builder, int start,
                                                  int end) const {
    const auto& column_nullable = assert_cast<const ColumnNullable&>(column);
    nested_serde->write_column_to_arrow(column_nullable.get_nested_column(),
                                        &column_nullable.get_null_map_data(), array_builder, start,
                                        end);
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
    col_const = col_const || is_column_const(nested_col);
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

void DataTypeNullableSerDe::write_one_cell_to_json(const IColumn& column, rapidjson::Value& result,
                                                   rapidjson::Document::AllocatorType& allocator,
                                                   int row_num) const {
    auto& col = static_cast<const ColumnNullable&>(column);
    auto& nested_col = col.get_nested_column();
    if (col.is_null_at(row_num)) {
        result.SetNull();
    } else {
        nested_serde->write_one_cell_to_json(nested_col, result, allocator, row_num);
    }
}

void DataTypeNullableSerDe::read_one_cell_from_json(IColumn& column,
                                                    const rapidjson::Value& result) const {
    auto& col = static_cast<ColumnNullable&>(column);
    auto& nested_col = col.get_nested_column();
    if (result.IsNull()) {
        col.insert_default();
    } else {
        // TODO sanitize data
        nested_serde->read_one_cell_from_json(nested_col, result);
        col.get_null_map_column().get_data().push_back(0);
    }
}

} // namespace vectorized
} // namespace doris
