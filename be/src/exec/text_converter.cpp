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

#include "text_converter.h"

#include <glog/logging.h>
#include <sql.h>
#include <stdint.h>

#include <algorithm>
#include <ostream>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "olap/hll.h"
#include "runtime/decimalv2_value.h"
#include "runtime/define_primitive_type.h"
#include "runtime/descriptors.h"
#include "runtime/types.h"
#include "util/slice.h"
#include "util/string_parser.hpp"
#include "vec/columns/column_array.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_struct.h"
#include "vec/columns/column_vector.h"
#include "vec/core/types.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {

TextConverter::TextConverter(char escape_char, char collection_delimiter, char map_kv_delimiter)
        : _escape_char(escape_char),
          _collection_delimiter(collection_delimiter),
          _map_kv_delimiter(map_kv_delimiter) {}

void TextConverter::write_string_column(const SlotDescriptor* slot_desc,
                                        vectorized::MutableColumnPtr* column_ptr, const char* data,
                                        size_t len, bool need_escape) {
    DCHECK(column_ptr->get()->is_nullable());
    auto* nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(column_ptr->get());
    if (need_escape) {
        unescape_string_on_spot(data, &len);
    }
    if ((len == 2 && data[0] == '\\' && data[1] == 'N') || len == SQL_NULL_DATA) {
        nullable_column->get_null_map_data().push_back(1);
        reinterpret_cast<vectorized::ColumnString&>(nullable_column->get_nested_column())
                .insert_default();
    } else {
        nullable_column->get_null_map_data().push_back(0);
        reinterpret_cast<vectorized::ColumnString&>(nullable_column->get_nested_column())
                .insert_data(data, len);
    }
}

bool TextConverter::_write_data(const TypeDescriptor& type_desc,
                                vectorized::IColumn* nullable_col_ptr, const char* data, size_t len,
                                bool copy_string, bool need_escape, size_t rows,
                                char array_delimiter) {
    vectorized::IColumn* col_ptr = nullable_col_ptr;
    // \N means it's NULL
    std::string col_type_name = col_ptr->get_name();
    bool is_null_able = typeid(*nullable_col_ptr) == typeid(vectorized::ColumnNullable);
    if (is_null_able) {
        auto* nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(nullable_col_ptr);
        if ((len == 2 && data[0] == '\\' && data[1] == 'N') || len == SQL_NULL_DATA) {
            nullable_column->insert_many_defaults(rows);
            return true;
        } else {
            auto& null_map = nullable_column->get_null_map_data();
            null_map.resize_fill(null_map.size() + rows, 0);
            col_ptr = &nullable_column->get_nested_column();
        }
    }

    StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
    size_t origin_size = col_ptr->size();
    // Parse the raw-text data. Translate the text string to internal format.
    switch (type_desc.type) {
    case TYPE_HLL: {
        HyperLogLog hyper_log_log(Slice(data, len));
        auto& hyper_data = reinterpret_cast<vectorized::ColumnHLL*>(col_ptr)->get_data();
        for (size_t i = 0; i < rows; ++i) {
            hyper_data.emplace_back(hyper_log_log);
        }
        break;
    }
    case TYPE_STRING:
    case TYPE_VARCHAR:
    case TYPE_CHAR: {
        if (need_escape) {
            unescape_string_on_spot(data, &len);
        }
        reinterpret_cast<vectorized::ColumnString*>(col_ptr)->insert_many_data(data, len, rows);
        break;
    }

    case TYPE_BOOLEAN: {
        bool num = StringParser::string_to_bool(data, len, &parse_result);
        if (parse_result != StringParser::PARSE_SUCCESS) {
            break;
        }
        reinterpret_cast<vectorized::ColumnVector<vectorized::UInt8>*>(col_ptr)
                ->get_data()
                .resize_fill(origin_size + rows, (uint8_t)num);
        break;
    }
    case TYPE_TINYINT: {
        int8_t num = StringParser::string_to_int<int8_t>(data, len, &parse_result);
        if (parse_result != StringParser::PARSE_SUCCESS) {
            break;
        }
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int8>*>(col_ptr)
                ->get_data()
                .resize_fill(origin_size + rows, num);
        break;
    }
    case TYPE_SMALLINT: {
        int16_t num = StringParser::string_to_int<int16_t>(data, len, &parse_result);
        if (parse_result != StringParser::PARSE_SUCCESS) {
            break;
        }
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int16>*>(col_ptr)
                ->get_data()
                .resize_fill(origin_size + rows, num);
        break;
    }
    case TYPE_INT: {
        int32_t num = StringParser::string_to_int<int32_t>(data, len, &parse_result);
        if (parse_result != StringParser::PARSE_SUCCESS) {
            break;
        }
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int32>*>(col_ptr)
                ->get_data()
                .resize_fill(origin_size + rows, num);
        break;
    }
    case TYPE_BIGINT: {
        int64_t num = StringParser::string_to_int<int64_t>(data, len, &parse_result);
        if (parse_result != StringParser::PARSE_SUCCESS) {
            break;
        }
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int64>*>(col_ptr)
                ->get_data()
                .resize_fill(origin_size + rows, num);
        break;
    }
    case TYPE_LARGEINT: {
        __int128 num = StringParser::string_to_int<__int128>(data, len, &parse_result);
        if (parse_result != StringParser::PARSE_SUCCESS) {
            break;
        }
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int128>*>(col_ptr)
                ->get_data()
                .resize_fill(origin_size + rows, num);
        break;
    }

    case TYPE_FLOAT: {
        float num = StringParser::string_to_float<float>(data, len, &parse_result);
        if (parse_result != StringParser::PARSE_SUCCESS) {
            break;
        }
        reinterpret_cast<vectorized::ColumnVector<vectorized::Float32>*>(col_ptr)
                ->get_data()
                .resize_fill(origin_size + rows, num);
        break;
    }
    case TYPE_DOUBLE: {
        double num = StringParser::string_to_float<double>(data, len, &parse_result);
        if (parse_result != StringParser::PARSE_SUCCESS) {
            break;
        }
        reinterpret_cast<vectorized::ColumnVector<vectorized::Float64>*>(col_ptr)
                ->get_data()
                .resize_fill(origin_size + rows, num);
        break;
    }
    case TYPE_DATE: {
        VecDateTimeValue ts_slot;
        if (!ts_slot.from_date_str(data, len)) {
            parse_result = StringParser::PARSE_FAILURE;
            break;
        }
        ts_slot.cast_to_date();
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int64>*>(col_ptr)
                ->get_data()
                .resize_fill(origin_size + rows, *reinterpret_cast<int64_t*>(&ts_slot));
        break;
    }
    case TYPE_DATEV2: {
        DateV2Value<DateV2ValueType> ts_slot;
        if (!ts_slot.from_date_str(data, len)) {
            parse_result = StringParser::PARSE_FAILURE;
            break;
        }
        uint32_t int_val = ts_slot.to_date_int_val();
        reinterpret_cast<vectorized::ColumnVector<vectorized::UInt32>*>(col_ptr)
                ->get_data()
                .resize_fill(origin_size + rows, int_val);
        break;
    }
    case TYPE_DATETIME: {
        VecDateTimeValue ts_slot;
        if (!ts_slot.from_date_str(data, len)) {
            parse_result = StringParser::PARSE_FAILURE;
            break;
        }
        ts_slot.to_datetime();
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int64>*>(col_ptr)
                ->get_data()
                .resize_fill(origin_size + rows, *reinterpret_cast<int64_t*>(&ts_slot));
        break;
    }
    case TYPE_DATETIMEV2: {
        DateV2Value<DateTimeV2ValueType> ts_slot;
        if (!ts_slot.from_date_str(data, len)) {
            parse_result = StringParser::PARSE_FAILURE;
            break;
        }
        uint64_t int_val = ts_slot.to_date_int_val();
        reinterpret_cast<vectorized::ColumnVector<vectorized::UInt64>*>(col_ptr)
                ->get_data()
                .resize_fill(origin_size + rows, int_val);
        break;
    }
    case TYPE_DECIMALV2: {
        DecimalV2Value decimal_slot;
        if (decimal_slot.parse_from_str(data, len)) {
            parse_result = StringParser::PARSE_FAILURE;
            break;
        }
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int128>*>(col_ptr)
                ->get_data()
                .resize_fill(origin_size + rows, decimal_slot.value());
        break;
    }
    case TYPE_DECIMAL32: {
        StringParser::ParseResult result = StringParser::PARSE_SUCCESS;
        int32_t value = StringParser::string_to_decimal<TYPE_DECIMAL32>(
                data, len, type_desc.precision, type_desc.scale, &result);
        if (result != StringParser::PARSE_SUCCESS) {
            parse_result = StringParser::PARSE_FAILURE;
            break;
        }
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int32>*>(col_ptr)
                ->get_data()
                .resize_fill(origin_size + rows, value);
        break;
    }
    case TYPE_DECIMAL64: {
        StringParser::ParseResult result = StringParser::PARSE_SUCCESS;
        int64_t value = StringParser::string_to_decimal<TYPE_DECIMAL64>(
                data, len, type_desc.precision, type_desc.scale, &result);
        if (result != StringParser::PARSE_SUCCESS) {
            parse_result = StringParser::PARSE_FAILURE;
            break;
        }
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int64>*>(col_ptr)
                ->get_data()
                .resize_fill(origin_size + rows, value);
        break;
    }
    case TYPE_DECIMAL128I: {
        StringParser::ParseResult result = StringParser::PARSE_SUCCESS;
        vectorized::Int128 value = StringParser::string_to_decimal<TYPE_DECIMAL128I>(
                data, len, type_desc.precision, type_desc.scale, &result);
        if (result != StringParser::PARSE_SUCCESS) {
            parse_result = StringParser::PARSE_FAILURE;
            break;
        }
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int128>*>(col_ptr)
                ->get_data()
                .resize_fill(origin_size + rows, value);
        break;
    }
    case TYPE_ARRAY: {
        auto col = reinterpret_cast<vectorized::ColumnArray*>(col_ptr);

        std::vector<std::pair<size_t, size_t>> ranges;
        for (size_t i = 0, from = 0; i <= len; i++) {
            if (i < len && data[i] != array_delimiter && data[i] != _collection_delimiter) {
                continue;
            }
            ranges.push_back({from, i - from});
            from = i + 1;
        }

        auto sub_type = type_desc.children[0];
        for (int i = 0; i < rows; i++) {
            for (auto range : ranges) {
                _write_data(sub_type, &col->get_data(), data + range.first, range.second,
                            copy_string, need_escape, 1, array_delimiter + 1);
            }
            col->get_offsets().push_back(col->get_offsets().back() + ranges.size());
        }

        break;
    }
    case TYPE_MAP: {
        auto col = reinterpret_cast<vectorized::ColumnMap*>(col_ptr);

        std::vector<std::array<size_t, 3>> ranges;
        for (size_t i = 0, from = 0, kv = 0; i <= len; i++) {
            /*
             *  In hive , when you special map key and value delimiter as ':'
             *  for map<int,timestamp> column , the query result is correct , but
             *  for map<timestamp, int> column and map<timestamp,timestamp> column , the query result is incorrect,
             *  because this field have many '_map_kv_delimiter'.
             *
             *  So i use 'kv <= from' in order to get _map_kv_delimiter that appears first.
             * */
            if (i < len && data[i] == _map_kv_delimiter && kv <= from) {
                kv = i;
                continue;
            }
            if ((i == len || data[i] == _collection_delimiter) && i >= kv + 1) {
                ranges.push_back({from, kv, i - 1});
                from = i + 1;
                kv = from;
            }
        }

        auto key_type = type_desc.children[0];
        auto value_type = type_desc.children[1];

        for (int i = 0; i < rows; i++) {
            for (auto range : ranges) {
                _write_data(key_type, &col->get_keys(), data + range[0], range[1] - range[0],
                            copy_string, need_escape, 1, array_delimiter + 1);

                _write_data(value_type, &col->get_values(), data + range[1] + 1,
                            range[2] - range[1], copy_string, need_escape, 1, array_delimiter + 1);
            }

            col->get_offsets().push_back(col->get_offsets().back() + ranges.size());
        }

        break;
    }
    case TYPE_STRUCT: {
        auto col = reinterpret_cast<vectorized::ColumnStruct*>(col_ptr);

        std::vector<std::pair<size_t, size_t>> ranges;
        for (size_t i = 0, from = 0; i <= len; i++) {
            if (i == len || data[i] == _collection_delimiter) {
                ranges.push_back({from, i - from});
                from = i + 1;
            }
        }
        for (int i = 0; i < rows; i++) {
            for (size_t loc = 0; loc < col->get_columns().size(); loc++) {
                _write_data(type_desc.children[loc], &col->get_column(loc),
                            data + ranges[loc].first, ranges[loc].second, copy_string, need_escape,
                            rows, array_delimiter + 1);
            }
        }
        break;
    }
    default:
        DCHECK(false) << "bad slot type: " << type_desc;
        break;
    }

    if (UNLIKELY(parse_result == StringParser::PARSE_FAILURE)) {
        if (is_null_able) {
            auto* nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(nullable_col_ptr);
            size_t size = nullable_column->get_null_map_data().size();
            doris::vectorized::NullMap& null_map_data = nullable_column->get_null_map_data();
            for (int i = 1; i <= rows; ++i) {
                null_map_data[size - i] = 1;
            }
            nullable_column->get_nested_column().insert_many_defaults(rows);
        }
        return false;
    }
    return true;
}

bool TextConverter::write_vec_column(const SlotDescriptor* slot_desc,
                                     vectorized::IColumn* nullable_col_ptr, const char* data,
                                     size_t len, bool copy_string, bool need_escape, size_t rows) {
    return _write_data(slot_desc->type(), nullable_col_ptr, data, len, copy_string, need_escape,
                       rows, '\2');
}

void TextConverter::unescape_string_on_spot(const char* src, size_t* len) {
    const char* start = src;
    char* dest_ptr = const_cast<char*>(src);
    const char* end = src + *len;
    bool escape_next_char = false;

    while (src < end) {
        if (*src == _escape_char) {
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

} // namespace doris
