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

#include "data_type_string_serde.h"

#include <assert.h>
#include <gen_cpp/types.pb.h>
#include <stddef.h>

#include "arrow/array/builder_binary.h"
#include "util/jsonb_document.h"
#include "util/jsonb_utils.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_string.h"
#include "vec/common/string_ref.h"

namespace doris {
namespace vectorized {
class Arena;

Status DataTypeStringSerDe::serialize_column_to_json(const IColumn& column, int start_idx,
                                                     int end_idx, BufferWritable& bw,
                                                     FormatOptions& options,
                                                     int nesting_level) const {
    SERIALIZE_COLUMN_TO_JSON();
}

Status DataTypeStringSerDe::serialize_one_cell_to_json(const IColumn& column, int row_num,
                                                       BufferWritable& bw, FormatOptions& options,
                                                       int nesting_level) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    const auto& value = assert_cast<const ColumnString&>(*ptr).get_data_at(row_num);
    bw.write(value.data, value.size);
    return Status::OK();
}

Status DataTypeStringSerDe::deserialize_column_from_json_vector(IColumn& column,
                                                                std::vector<Slice>& slices,
                                                                int* num_deserialized,
                                                                const FormatOptions& options,
                                                                int nesting_level) const {
    DESERIALIZE_COLUMN_FROM_JSON_VECTOR()
    return Status::OK();
}
static void escape_string(const char* src, size_t& len, char escape_char) {
    const char* start = src;
    char* dest_ptr = const_cast<char*>(src);
    const char* end = src + len;
    bool escape_next_char = false;

    while (src < end) {
        if (*src == escape_char) {
            escape_next_char = !escape_next_char;
        } else {
            escape_next_char = false;
        }

        if (escape_next_char) {
            ++src;
        } else {
            *dest_ptr++ = *src++;
        }
    }

    len = dest_ptr - start;
}

Status DataTypeStringSerDe::deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                                           const FormatOptions& options,
                                                           int nesting_level) const {
    auto& column_data = assert_cast<ColumnString&>(column);

    /*
     * For strings in the json complex type, we remove double quotes by default.
     *
     * Because when querying complex types, such as selecting complexColumn from table,
     * we will add double quotes to the strings in the complex type.
     *
     * For the map<string,int> column, insert { "abc" : 1, "hello",2 }.
     * If you do not remove the double quotes, it will display {""abc"":1,""hello"": 2 },
     * remove the double quotes to display { "abc" : 1, "hello",2 }.
     *
     */
    if (nesting_level >= 2) {
        slice.trim_quote();
    }
    if (options.escape_char != 0) {
        escape_string(slice.data, slice.size, options.escape_char);
    }
    column_data.insert_data(slice.data, slice.size);
    return Status::OK();
}

Status DataTypeStringSerDe::write_column_to_pb(const IColumn& column, PValues& result, int start,
                                               int end) const {
    result.mutable_bytes_value()->Reserve(end - start);
    auto ptype = result.mutable_type();
    ptype->set_id(PGenericType::STRING);
    for (size_t row_num = start; row_num < end; ++row_num) {
        StringRef data = column.get_data_at(row_num);
        result.add_string_value(data.to_string());
    }
    return Status::OK();
}
Status DataTypeStringSerDe::read_column_from_pb(IColumn& column, const PValues& arg) const {
    auto& col = reinterpret_cast<ColumnString&>(column);
    col.reserve(arg.string_value_size());
    for (int i = 0; i < arg.string_value_size(); ++i) {
        column.insert_data(arg.string_value(i).c_str(), arg.string_value(i).size());
    }
    return Status::OK();
}

void DataTypeStringSerDe::write_one_cell_to_jsonb(const IColumn& column, JsonbWriter& result,
                                                  Arena* mem_pool, int32_t col_id,
                                                  int row_num) const {
    result.writeKey(col_id);
    const auto& data_ref = column.get_data_at(row_num);
    result.writeStartBinary();
    result.writeBinary(reinterpret_cast<const char*>(data_ref.data), data_ref.size);
    result.writeEndBinary();
}
void DataTypeStringSerDe::read_one_cell_from_jsonb(IColumn& column, const JsonbValue* arg) const {
    assert(arg->isBinary());
    auto& col = reinterpret_cast<ColumnString&>(column);
    auto blob = static_cast<const JsonbBlobVal*>(arg);
    col.insert_data(blob->getBlob(), blob->getBlobLen());
}

void DataTypeStringSerDe::write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                                arrow::ArrayBuilder* array_builder, int start,
                                                int end) const {
    const auto& string_column = assert_cast<const ColumnString&>(column);
    auto& builder = assert_cast<arrow::StringBuilder&>(*array_builder);
    for (size_t string_i = start; string_i < end; ++string_i) {
        if (null_map && (*null_map)[string_i]) {
            checkArrowStatus(builder.AppendNull(), column.get_name(),
                             array_builder->type()->name());
            continue;
        }
        std::string_view string_ref = string_column.get_data_at(string_i).to_string_view();
        checkArrowStatus(builder.Append(string_ref.data(), string_ref.size()), column.get_name(),
                         array_builder->type()->name());
    }
}

void DataTypeStringSerDe::read_column_from_arrow(IColumn& column, const arrow::Array* arrow_array,
                                                 int start, int end,
                                                 const cctz::time_zone& ctz) const {
    auto& column_chars_t = assert_cast<ColumnString&>(column).get_chars();
    auto& column_offsets = assert_cast<ColumnString&>(column).get_offsets();
    if (arrow_array->type_id() == arrow::Type::STRING ||
        arrow_array->type_id() == arrow::Type::BINARY) {
        auto concrete_array = dynamic_cast<const arrow::BinaryArray*>(arrow_array);
        std::shared_ptr<arrow::Buffer> buffer = concrete_array->value_data();

        for (size_t offset_i = start; offset_i < end; ++offset_i) {
            if (!concrete_array->IsNull(offset_i)) {
                const auto* raw_data = buffer->data() + concrete_array->value_offset(offset_i);
                column_chars_t.insert(raw_data, raw_data + concrete_array->value_length(offset_i));
            }
            column_offsets.emplace_back(column_chars_t.size());
        }
    } else if (arrow_array->type_id() == arrow::Type::FIXED_SIZE_BINARY) {
        auto concrete_array = dynamic_cast<const arrow::FixedSizeBinaryArray*>(arrow_array);
        uint32_t width = concrete_array->byte_width();
        const auto* array_data = concrete_array->GetValue(start);

        for (size_t offset_i = 0; offset_i < end - start; ++offset_i) {
            if (!concrete_array->IsNull(offset_i)) {
                const auto* raw_data = array_data + (offset_i * width);
                column_chars_t.insert(raw_data, raw_data + width);
            }
            column_offsets.emplace_back(column_chars_t.size());
        }
    }
}

template <bool is_binary_format>
Status DataTypeStringSerDe::_write_column_to_mysql(const IColumn& column,
                                                   MysqlRowBuffer<is_binary_format>& result,
                                                   int row_idx, bool col_const) const {
    auto& col = assert_cast<const ColumnString&>(column);
    const auto col_index = index_check_const(row_idx, col_const);
    const auto string_val = col.get_data_at(col_index);
    if (string_val.data == nullptr) {
        if (string_val.size == 0) {
            // 0x01 is a magic num, not useful actually, just for present ""
            char* tmp_val = reinterpret_cast<char*>(0x01);
            if (UNLIKELY(0 != result.push_string(tmp_val, string_val.size))) {
                return Status::InternalError("pack mysql buffer failed.");
            }
        } else {
            if (UNLIKELY(0 != result.push_null())) {
                return Status::InternalError("pack mysql buffer failed.");
            }
        }
    } else {
        if (UNLIKELY(0 != result.push_string(string_val.data, string_val.size))) {
            return Status::InternalError("pack mysql buffer failed.");
        }
    }
    return Status::OK();
}

Status DataTypeStringSerDe::write_column_to_mysql(const IColumn& column,
                                                  MysqlRowBuffer<true>& row_buffer, int row_idx,
                                                  bool col_const) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const);
}

Status DataTypeStringSerDe::write_column_to_mysql(const IColumn& column,
                                                  MysqlRowBuffer<false>& row_buffer, int row_idx,
                                                  bool col_const) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const);
}

Status DataTypeStringSerDe::write_column_to_orc(const IColumn& column, const NullMap* null_map,
                                                orc::ColumnVectorBatch* orc_col_batch, int start,
                                                int end,
                                                std::vector<StringRef>& buffer_list) const {
    auto& col_data = assert_cast<const ColumnString&>(column);
    orc::StringVectorBatch* cur_batch = dynamic_cast<orc::StringVectorBatch*>(orc_col_batch);

    for (size_t row_id = start; row_id < end; row_id++) {
        const auto& ele = col_data.get_data_at(row_id);
        cur_batch->data[row_id] = const_cast<char*>(ele.data);
        cur_batch->length[row_id] = ele.size;
    }

    cur_batch->numElements = end - start;
    return Status::OK();
}

} // namespace vectorized
} // namespace doris