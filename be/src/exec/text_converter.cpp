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

#include <sql.h>

#include "runtime/decimalv2_value.h"
#include "runtime/descriptors.h"
#include "runtime/mem_pool.h"
#include "runtime/string_value.h"
#include "runtime/tuple.h"
#include "util/string_parser.hpp"
#include "util/types.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_nullable.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {

TextConverter::TextConverter(char escape_char) : _escape_char(escape_char) {}

bool TextConverter::write_vec_column(const SlotDescriptor* slot_desc,
                                     vectorized::IColumn* nullable_col_ptr, const char* data,
                                     size_t len, bool copy_string, bool need_escape, size_t rows) {
    vectorized::IColumn* col_ptr = nullable_col_ptr;
    // \N means it's NULL
    if (slot_desc->is_nullable()) {
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
    switch (slot_desc->type().type) {
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
        reinterpret_cast<vectorized::ColumnVector<vectorized::UInt8>*>(col_ptr)
                ->get_data()
                .resize_fill(origin_size + rows, (uint8_t)num);
        break;
    }
    case TYPE_TINYINT: {
        int8_t num = StringParser::string_to_int<int8_t>(data, len, &parse_result);
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int8>*>(col_ptr)
                ->get_data()
                .resize_fill(origin_size + rows, num);
        break;
    }
    case TYPE_SMALLINT: {
        int16_t num = StringParser::string_to_int<int16_t>(data, len, &parse_result);
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int16>*>(col_ptr)
                ->get_data()
                .resize_fill(origin_size + rows, num);
        break;
    }
    case TYPE_INT: {
        int32_t num = StringParser::string_to_int<int32_t>(data, len, &parse_result);
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int32>*>(col_ptr)
                ->get_data()
                .resize_fill(origin_size + rows, num);
        break;
    }
    case TYPE_BIGINT: {
        int64_t num = StringParser::string_to_int<int64_t>(data, len, &parse_result);
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int64>*>(col_ptr)
                ->get_data()
                .resize_fill(origin_size + rows, num);
        break;
    }
    case TYPE_LARGEINT: {
        __int128 num = StringParser::string_to_int<__int128>(data, len, &parse_result);
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int128>*>(col_ptr)
                ->get_data()
                .resize_fill(origin_size + rows, num);
        break;
    }

    case TYPE_FLOAT: {
        float num = StringParser::string_to_float<float>(data, len, &parse_result);
        reinterpret_cast<vectorized::ColumnVector<vectorized::Float32>*>(col_ptr)
                ->get_data()
                .resize_fill(origin_size + rows, num);
        break;
    }
    case TYPE_DOUBLE: {
        double num = StringParser::string_to_float<double>(data, len, &parse_result);
        reinterpret_cast<vectorized::ColumnVector<vectorized::Float64>*>(col_ptr)
                ->get_data()
                .resize_fill(origin_size + rows, num);
        break;
    }
    case TYPE_DATE: {
        vectorized::VecDateTimeValue ts_slot;
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

    case TYPE_DATETIME: {
        vectorized::VecDateTimeValue ts_slot;
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

    default:
        DCHECK(false) << "bad slot type: " << slot_desc->type();
        break;
    }

    if (UNLIKELY(parse_result == StringParser::PARSE_FAILURE)) {
        if (true == slot_desc->is_nullable()) {
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

void TextConverter::unescape_string(StringValue* value, MemPool* pool) {
    char* new_data = reinterpret_cast<char*>(pool->allocate(value->len));
    unescape_string(value->ptr, new_data, &value->len);
    value->ptr = new_data;
}

void TextConverter::unescape_string(const char* src, char* dest, size_t* len) {
    char* dest_ptr = dest;
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

    char* dest_start = reinterpret_cast<char*>(dest);
    *len = dest_ptr - dest_start;
}

void TextConverter::unescape_string_on_spot(const char* src, size_t* len) {
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

    *len = dest_ptr - src;
}

} // namespace doris
