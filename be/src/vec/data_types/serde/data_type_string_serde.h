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

#include <arrow/array/array_base.h>
#include <arrow/array/array_binary.h>
#include <arrow/array/builder_base.h>
#include <arrow/array/builder_binary.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "common/status.h"
#include "data_type_serde.h"
#include "util/jsonb_writer.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_fixed_length_object.h"
#include "vec/columns/column_string.h"
#include "vec/core/types.h"

namespace doris {
class PValues;
class JsonbValue;

namespace vectorized {
class IColumn;
class Arena;

inline void escape_string(const char* src, size_t& len, char escape_char) {
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

template <typename ColumnType>
class DataTypeStringSerDeBase : public DataTypeSerDe {
public:
    DataTypeStringSerDeBase(int nesting_level = 1) : DataTypeSerDe(nesting_level) {};

    Status serialize_one_cell_to_json(const IColumn& column, int row_num, BufferWritable& bw,
                                      FormatOptions& options) const override {
        auto result = check_column_const_set_readability(column, row_num);
        ColumnPtr ptr = result.first;
        row_num = result.second;
        const auto& value = assert_cast<const ColumnType&>(*ptr).get_data_at(row_num);

        if (_nesting_level > 1) {
            bw.write('"');
        }
        if constexpr (std::is_same_v<ColumnType, ColumnString>) {
            if (options.escape_char != 0) {
                // we should make deal with some special characters in json str if we have escape_char
                StringRef str_ref = value;
                write_with_escaped_char_to_json(str_ref, bw);
            } else {
                bw.write(value.data, value.size);
            }
        } else {
            bw.write(value.data, value.size);
        }
        if (_nesting_level > 1) {
            bw.write('"');
        }

        return Status::OK();
    }

    inline void write_with_escaped_char_to_json(StringRef value, BufferWritable& bw) const {
        for (char it : value) {
            switch (it) {
            case '\b':
                bw.write("\\b", 2);
                break;
            case '\f':
                bw.write("\\f", 2);
                break;
            case '\n':
                bw.write("\\n", 2);
                break;
            case '\r':
                bw.write("\\r", 2);
                break;
            case '\t':
                bw.write("\\t", 2);
                break;
            case '\\':
                bw.write("\\\\", 2);
                break;
            case '"':
                bw.write("\\\"", 2);
                break;
            default:
                bw.write(it);
            }
        }
    }

    Status serialize_column_to_json(const IColumn& column, int start_idx, int end_idx,
                                    BufferWritable& bw, FormatOptions& options) const override {
        SERIALIZE_COLUMN_TO_JSON();
    }

    Status deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                          const FormatOptions& options) const override {
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
        if (_nesting_level >= 2) {
            slice.trim_quote();
        }
        if (options.escape_char != 0) {
            escape_string(slice.data, slice.size, options.escape_char);
        }
        assert_cast<ColumnType&>(column).insert_data(slice.data, slice.size);
        return Status::OK();
    }

    Status deserialize_column_from_json_vector(IColumn& column, std::vector<Slice>& slices,
                                               int* num_deserialized,
                                               const FormatOptions& options) const override {
        DESERIALIZE_COLUMN_FROM_JSON_VECTOR()
        return Status::OK();
    }

    Status write_column_to_pb(const IColumn& column, PValues& result, int start,
                              int end) const override {
        result.mutable_string_value()->Reserve(end - start);
        auto* ptype = result.mutable_type();
        ptype->set_id(PGenericType::STRING);
        for (size_t row_num = start; row_num < end; ++row_num) {
            StringRef data = column.get_data_at(row_num);
            result.add_string_value(data.to_string());
        }
        return Status::OK();
    }

    Status deserialize_column_from_fixed_json(IColumn& column, Slice& slice, int rows,
                                              int* num_deserialized,
                                              const FormatOptions& options) const override {
        Status st = deserialize_one_cell_from_json(column, slice, options);
        if (!st.ok()) {
            return st;
        }

        DataTypeStringSerDeBase::insert_column_last_value_multiple_times(column, rows - 1);
        *num_deserialized = rows;
        return Status::OK();
    }

    void insert_column_last_value_multiple_times(IColumn& column, int times) const override {
        auto& col = static_cast<ColumnString&>(column);
        auto sz = col.size();

        StringRef ref = col.get_data_at(sz - 1);
        String str(ref.data, ref.size);
        std::vector<StringRef> refs(times, {str.data(), str.size()});

        col.insert_many_strings(refs.data(), refs.size());
    }

    Status read_column_from_pb(IColumn& column, const PValues& arg) const override {
        auto& column_dest = assert_cast<ColumnType&>(column);
        column_dest.reserve(column_dest.size() + arg.string_value_size());
        for (int i = 0; i < arg.string_value_size(); ++i) {
            column_dest.insert_data(arg.string_value(i).c_str(), arg.string_value(i).size());
        }
        return Status::OK();
    }
    void write_one_cell_to_jsonb(const IColumn& column, JsonbWriter& result, Arena* mem_pool,
                                 int32_t col_id, int row_num) const override {
        result.writeKey(col_id);
        const auto& data_ref = column.get_data_at(row_num);
        result.writeStartBinary();
        result.writeBinary(reinterpret_cast<const char*>(data_ref.data), data_ref.size);
        result.writeEndBinary();
    }

    void read_one_cell_from_jsonb(IColumn& column, const JsonbValue* arg) const override {
        assert(arg->isBinary());
        const auto* blob = static_cast<const JsonbBlobVal*>(arg);
        assert_cast<ColumnType&>(column).insert_data(blob->getBlob(), blob->getBlobLen());
    }

    void write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                               arrow::ArrayBuilder* array_builder, int start, int end,
                               const cctz::time_zone& ctz) const override {
        const auto& string_column = assert_cast<const ColumnType&>(column);
        auto& builder = assert_cast<arrow::StringBuilder&>(*array_builder);
        for (size_t string_i = start; string_i < end; ++string_i) {
            if (null_map && (*null_map)[string_i]) {
                checkArrowStatus(builder.AppendNull(), column.get_name(),
                                 array_builder->type()->name());
                continue;
            }
            auto string_ref = string_column.get_data_at(string_i);
            checkArrowStatus(builder.Append(string_ref.data, string_ref.size), column.get_name(),
                             array_builder->type()->name());
        }
    }
    void read_column_from_arrow(IColumn& column, const arrow::Array* arrow_array, int start,
                                int end, const cctz::time_zone& ctz) const override {
        if (arrow_array->type_id() == arrow::Type::STRING ||
            arrow_array->type_id() == arrow::Type::BINARY) {
            const auto* concrete_array = dynamic_cast<const arrow::BinaryArray*>(arrow_array);
            std::shared_ptr<arrow::Buffer> buffer = concrete_array->value_data();

            for (size_t offset_i = start; offset_i < end; ++offset_i) {
                if (!concrete_array->IsNull(offset_i)) {
                    const auto* raw_data = buffer->data() + concrete_array->value_offset(offset_i);
                    assert_cast<ColumnType&>(column).insert_data(
                            (char*)raw_data, concrete_array->value_length(offset_i));
                } else {
                    assert_cast<ColumnType&>(column).insert_default();
                }
            }
        } else if (arrow_array->type_id() == arrow::Type::FIXED_SIZE_BINARY) {
            const auto* concrete_array =
                    dynamic_cast<const arrow::FixedSizeBinaryArray*>(arrow_array);
            uint32_t width = concrete_array->byte_width();
            const auto* array_data = concrete_array->GetValue(start);

            for (size_t offset_i = 0; offset_i < end - start; ++offset_i) {
                if (!concrete_array->IsNull(offset_i)) {
                    const auto* raw_data = array_data + (offset_i * width);
                    assert_cast<ColumnType&>(column).insert_data((char*)raw_data, width);
                } else {
                    assert_cast<ColumnType&>(column).insert_default();
                }
            }
        }
    }

    Status write_column_to_mysql(const IColumn& column, MysqlRowBuffer<true>& row_buffer,
                                 int row_idx, bool col_const,
                                 const FormatOptions& options) const override {
        return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
    }
    Status write_column_to_mysql(const IColumn& column, MysqlRowBuffer<false>& row_buffer,
                                 int row_idx, bool col_const,
                                 const FormatOptions& options) const override {
        return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
    }

    Status write_column_to_orc(const std::string& timezone, const IColumn& column,
                               const NullMap* null_map, orc::ColumnVectorBatch* orc_col_batch,
                               int start, int end,
                               std::vector<StringRef>& buffer_list) const override {
        auto* cur_batch = dynamic_cast<orc::StringVectorBatch*>(orc_col_batch);

        for (size_t row_id = start; row_id < end; row_id++) {
            const auto& ele = assert_cast<const ColumnType&>(column).get_data_at(row_id);
            cur_batch->data[row_id] = const_cast<char*>(ele.data);
            cur_batch->length[row_id] = ele.size;
        }

        cur_batch->numElements = end - start;
        return Status::OK();
    }
    Status write_one_cell_to_json(const IColumn& column, rapidjson::Value& result,
                                  rapidjson::Document::AllocatorType& allocator, Arena& mem_pool,
                                  int row_num) const override {
        const auto& col = assert_cast<const ColumnType&>(column);
        const auto& data_ref = col.get_data_at(row_num);
        result.SetString(data_ref.data, data_ref.size);
        return Status::OK();
    }
    Status read_one_cell_from_json(IColumn& column, const rapidjson::Value& result) const override {
        auto& col = assert_cast<ColumnType&>(column);
        if (!result.IsString()) {
            rapidjson::StringBuffer buffer;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
            result.Accept(writer);
            col.insert_data(buffer.GetString(), buffer.GetSize());
            return Status::OK();
        }
        col.insert_data(result.GetString(), result.GetStringLength());
        return Status::OK();
    }

private:
    template <bool is_binary_format>
    Status _write_column_to_mysql(const IColumn& column, MysqlRowBuffer<is_binary_format>& result,
                                  int row_idx, bool col_const, const FormatOptions& options) const {
        const auto col_index = index_check_const(row_idx, col_const);
        const auto string_val = assert_cast<const ColumnType&>(column).get_data_at(col_index);
        result.push_string(string_val.data, string_val.size);
        return Status::OK();
    }
};

using DataTypeStringSerDe = DataTypeStringSerDeBase<ColumnString>;
using DataTypeFixedLengthObjectSerDe = DataTypeStringSerDeBase<ColumnFixedLengthObject>;
} // namespace vectorized
} // namespace doris
