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

#include "common/status.h"
#include "data_type_serde.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_fixed_length_object.h"
#include "vec/columns/column_string.h"
#include "vec/core/types.h"

namespace doris {
class PValues;
struct JsonbValue;

namespace vectorized {
class IColumn;
class Arena;
#include "common/compile_check_begin.h"

inline void escape_string(char* src, size_t* len, char escape_char) {
    const char* start = src;
    char* dest_ptr = src;
    const char* end = src + *len;
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
            if (dest_ptr != src) {
                *dest_ptr = *src;
            }
            dest_ptr++;
            src++;
        }
    }

    *len = dest_ptr - start;
}

// specially escape quote with double quote
inline void escape_string_for_csv(char* src, size_t* len, char escape_char, char quote_char) {
    const char* start = src;
    char* dest_ptr = src;
    const char* end = src + *len;
    bool escape_next_char = false;

    while (src < end) {
        if ((src < end - 1 && *src == quote_char && *(src + 1) == quote_char) ||
            *src == escape_char) {
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

    *len = dest_ptr - start;
}

template <typename ColumnType>
class DataTypeStringSerDeBase : public DataTypeSerDe {
    using ColumnStrType = ColumnType;

public:
    DataTypeStringSerDeBase(int nesting_level = 1) : DataTypeSerDe(nesting_level) {};

    std::string get_name() const override { return "String"; }

    Status from_string(StringRef& str, IColumn& column,
                       const FormatOptions& options) const override;

    Status serialize_one_cell_to_json(const IColumn& column, int64_t row_num, BufferWritable& bw,
                                      FormatOptions& options) const override;

    Status serialize_one_cell_to_hive_text(
            const IColumn& column, int64_t row_num, BufferWritable& bw, FormatOptions& options,
            int hive_text_complex_type_delimiter_level = 1) const override;
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

    inline void write_with_escaped_char_to_hive_text(StringRef value, BufferWritable& bw,
                                                     char escape_char,
                                                     const bool need_escape[]) const {
        for (char it : value) {
            if (need_escape[it & 0xff]) {
                bw.write(escape_char);
            }
            bw.write(it);
        }
    }

    Status serialize_column_to_json(const IColumn& column, int64_t start_idx, int64_t end_idx,
                                    BufferWritable& bw, FormatOptions& options) const override;

    Status deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                          const FormatOptions& options) const override;

    Status deserialize_one_cell_from_csv(IColumn& column, Slice& slice,
                                         const FormatOptions& options) const override;

    Status deserialize_one_cell_from_hive_text(
            IColumn& column, Slice& slice, const FormatOptions& options,
            int hive_text_complex_type_delimiter_level = 1) const override;

    Status deserialize_column_from_json_vector(IColumn& column, std::vector<Slice>& slices,
                                               uint64_t* num_deserialized,
                                               const FormatOptions& options) const override;

    Status write_column_to_pb(const IColumn& column, PValues& result, int64_t start,
                              int64_t end) const override;

    Status deserialize_column_from_fixed_json(IColumn& column, Slice& slice, uint64_t rows,
                                              uint64_t* num_deserialized,
                                              const FormatOptions& options) const override;

    void insert_column_last_value_multiple_times(IColumn& column, uint64_t times) const override;

    Status read_column_from_pb(IColumn& column, const PValues& arg) const override;

    Status serialize_column_to_jsonb(const IColumn& from_column, int64_t row_num,
                                     JsonbWriter& writer) const override;

    Status deserialize_column_from_jsonb(IColumn& column, const JsonbValue* jsonb_value,
                                         CastParameters& castParms) const override;

    void write_one_cell_to_jsonb(const IColumn& column, JsonbWriter& result, Arena& mem_pool,
                                 int32_t col_id, int64_t row_num) const override;

    void read_one_cell_from_jsonb(IColumn& column, const JsonbValue* arg) const override;

    template <typename BuilderType>
    Status write_column_to_arrow_impl(const IColumn& column, const NullMap* null_map,
                                      BuilderType& builder, int64_t start, int64_t end) const;

    Status write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                 arrow::ArrayBuilder* array_builder, int64_t start, int64_t end,
                                 const cctz::time_zone& ctz) const override;

    Status read_column_from_arrow(IColumn& column, const arrow::Array* arrow_array, int64_t start,
                                  int64_t end, const cctz::time_zone& ctz) const override;

    Status write_column_to_mysql(const IColumn& column, MysqlRowBuffer<true>& row_buffer,
                                 int64_t row_idx, bool col_const,
                                 const FormatOptions& options) const override;

    Status write_column_to_mysql(const IColumn& column, MysqlRowBuffer<false>& row_buffer,
                                 int64_t row_idx, bool col_const,
                                 const FormatOptions& options) const override;

    Status write_column_to_orc(const std::string& timezone, const IColumn& column,
                               const NullMap* null_map, orc::ColumnVectorBatch* orc_col_batch,
                               int64_t start, int64_t end, vectorized::Arena& arena) const override;

    void write_one_cell_to_binary(const IColumn& src_column, ColumnString::Chars& chars,
                                  int64_t row_num) const override {
        const uint8_t type = static_cast<uint8_t>(FieldType::OLAP_FIELD_TYPE_STRING);
        const auto& col = assert_cast<const ColumnType&>(src_column);
        const auto& data_ref = col.get_data_at(row_num);
        const size_t data_size = data_ref.size;

        const size_t old_size = chars.size();
        const size_t new_size = old_size + sizeof(uint8_t) + sizeof(size_t) + data_ref.size;
        chars.resize(new_size);

        memcpy(chars.data() + old_size, reinterpret_cast<const char*>(&type), sizeof(uint8_t));
        memcpy(chars.data() + old_size + sizeof(uint8_t), reinterpret_cast<const char*>(&data_size),
               sizeof(size_t));
        memcpy(chars.data() + old_size + sizeof(uint8_t) + sizeof(size_t), data_ref.data,
               data_size);
    }

private:
    template <bool is_binary_format>
    Status _write_column_to_mysql(const IColumn& column, MysqlRowBuffer<is_binary_format>& result,
                                  int64_t row_idx, bool col_const,
                                  const FormatOptions& options) const {
        const auto col_index = index_check_const(row_idx, col_const);
        const auto string_val = assert_cast<const ColumnType&>(column).get_data_at(col_index);
        result.push_string(string_val.data, string_val.size);
        return Status::OK();
    }
};

using DataTypeStringSerDe = DataTypeStringSerDeBase<ColumnString>;
using DataTypeFixedLengthObjectSerDe = DataTypeStringSerDeBase<ColumnFixedLengthObject>;
#include "common/compile_check_end.h"
} // namespace vectorized
} // namespace doris
