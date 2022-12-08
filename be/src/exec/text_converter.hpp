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

#include <sql.h>

#include <boost/algorithm/string.hpp>

#include "runtime/datetime_value.h"
#include "runtime/decimalv2_value.h"
#include "runtime/descriptors.h"
#include "runtime/mem_pool.h"
#include "runtime/string_value.h"
#include "runtime/tuple.h"
#include "text_converter.h"
#include "util/binary_cast.hpp"
#include "util/string_parser.hpp"
#include "util/types.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_nullable.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {

// Note: this function has a codegen'd version.  Changing this function requires
// corresponding changes to CodegenWriteSlot.
inline bool TextConverter::write_slot(const SlotDescriptor* slot_desc, Tuple* tuple,
                                      const char* data, int len, bool copy_string, bool need_escape,
                                      MemPool* pool) {
    //Small batch import only \N is considered to be NULL, there is no replace_value function for batch import
    if (true == slot_desc->is_nullable()) {
        if (len == 2 && data[0] == '\\' && data[1] == 'N') {
            tuple->set_null(slot_desc->null_indicator_offset());
            return true;
        } else {
            tuple->set_not_null(slot_desc->null_indicator_offset());
        }
    }

    StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
    void* slot = tuple->get_slot(slot_desc->tuple_offset());

    // Parse the raw-text data. Translate the text string to internal format.
    switch (slot_desc->type().type) {
    case TYPE_HLL:
    case TYPE_VARCHAR:
    case TYPE_CHAR:
    case TYPE_STRING: {
        StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
        str_slot->ptr = const_cast<char*>(data);
        str_slot->len = len;
        if (len != 0 && (copy_string || need_escape)) {
            DCHECK(pool != NULL);
            char* slot_data = reinterpret_cast<char*>(pool->allocate(len));

            if (need_escape) {
                unescape_string(data, slot_data, &str_slot->len);
            } else {
                memcpy(slot_data, data, str_slot->len);
            }

            str_slot->ptr = slot_data;
        }

        break;
    }

    case TYPE_BOOLEAN:
        *reinterpret_cast<bool*>(slot) = StringParser::string_to_bool(data, len, &parse_result);
        break;

    case TYPE_TINYINT:
        *reinterpret_cast<int8_t*>(slot) =
                StringParser::string_to_int<int8_t>(data, len, &parse_result);
        break;

    case TYPE_SMALLINT:
        *reinterpret_cast<int16_t*>(slot) =
                StringParser::string_to_int<int16_t>(data, len, &parse_result);
        break;

    case TYPE_INT:
        *reinterpret_cast<int32_t*>(slot) =
                StringParser::string_to_int<int32_t>(data, len, &parse_result);
        break;

    case TYPE_BIGINT:
        *reinterpret_cast<int64_t*>(slot) =
                StringParser::string_to_int<int64_t>(data, len, &parse_result);
        break;

    case TYPE_LARGEINT: {
        __int128 tmp = StringParser::string_to_int<__int128>(data, len, &parse_result);
        memcpy(slot, &tmp, sizeof(tmp));
        break;
    }

    case TYPE_FLOAT:
        *reinterpret_cast<float*>(slot) =
                StringParser::string_to_float<float>(data, len, &parse_result);
        break;

    case TYPE_DOUBLE:
        *reinterpret_cast<double*>(slot) =
                StringParser::string_to_float<double>(data, len, &parse_result);
        break;

    case TYPE_DATE: {
        DateTimeValue* ts_slot = reinterpret_cast<DateTimeValue*>(slot);
        if (!ts_slot->from_date_str(data, len)) {
            parse_result = StringParser::PARSE_FAILURE;
            break;
        }

        ts_slot->cast_to_date();
        break;
    }

    case TYPE_DATETIME: {
        DateTimeValue* ts_slot = reinterpret_cast<DateTimeValue*>(slot);
        if (!ts_slot->from_date_str(data, len)) {
            parse_result = StringParser::PARSE_FAILURE;
        }

        ts_slot->to_datetime();
        break;
    }

    case TYPE_DECIMALV2: {
        DecimalV2Value decimal_slot;

        if (decimal_slot.parse_from_str(data, len)) {
            parse_result = StringParser::PARSE_FAILURE;
        }

        *reinterpret_cast<PackedInt128*>(slot) = decimal_slot.value();
        break;
    }

    default:
        DCHECK(false) << "bad slot type: " << slot_desc->type();
        break;
    }

    // TODO: add warning for overflow case
    if (parse_result != StringParser::PARSE_SUCCESS) {
        tuple->set_null(slot_desc->null_indicator_offset());
        return false;
    }

    return true;
}

inline void TextConverter::write_string_column(const SlotDescriptor* slot_desc,
                                               vectorized::MutableColumnPtr* column_ptr,
                                               const char* data, size_t len) {
    DCHECK(column_ptr->get()->is_nullable());
    auto* nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(column_ptr->get());
    if (len == 2 && data[0] == '\\' && data[1] == 'N') {
        nullable_column->get_null_map_data().push_back(1);
        reinterpret_cast<vectorized::ColumnString&>(nullable_column->get_nested_column())
                .insert_default();
    } else {
        nullable_column->get_null_map_data().push_back(0);
        reinterpret_cast<vectorized::ColumnString&>(nullable_column->get_nested_column())
                .insert_data(data, len);
    }
}

inline bool TextConverter::write_column(const SlotDescriptor* slot_desc,
                                        vectorized::MutableColumnPtr* column_ptr, const char* data,
                                        size_t len, bool copy_string, bool need_escape) {
    vectorized::IColumn* nullable_col_ptr = column_ptr->get();
    return write_vec_column(slot_desc, nullable_col_ptr, data, len, copy_string, need_escape);
}

inline bool TextConverter::write_vec_column(const SlotDescriptor* slot_desc,
                                            vectorized::IColumn* nullable_col_ptr, const char* data,
                                            size_t len, bool copy_string, bool need_escape) {
    vectorized::IColumn* col_ptr = nullable_col_ptr;
    // \N means it's NULL
    if (slot_desc->is_nullable()) {
        auto* nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(nullable_col_ptr);
        if ((len == 2 && data[0] == '\\' && data[1] == 'N') || len == SQL_NULL_DATA) {
            nullable_column->insert_data(nullptr, 0);
            return true;
        } else {
            nullable_column->get_null_map_data().push_back(0);
            col_ptr = &nullable_column->get_nested_column();
        }
    }

    StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
    // Parse the raw-text data. Translate the text string to internal format.
    switch (slot_desc->type().type) {
    case TYPE_HLL: {
        reinterpret_cast<vectorized::ColumnHLL*>(col_ptr)->get_data().emplace_back(
                HyperLogLog(Slice(data, len)));
        break;
    }
    case TYPE_STRING:
    case TYPE_VARCHAR:
    case TYPE_CHAR: {
        if (need_escape) {
            unescape_string_on_spot(data, &len);
        }
        reinterpret_cast<vectorized::ColumnString*>(col_ptr)->insert_data(data, len);
        break;
    }

    case TYPE_BOOLEAN: {
        bool num = StringParser::string_to_bool(data, len, &parse_result);
        reinterpret_cast<vectorized::ColumnVector<vectorized::UInt8>*>(col_ptr)->insert_value(
                (uint8_t)num);
        break;
    }
    case TYPE_TINYINT: {
        int8_t num = StringParser::string_to_int<int8_t>(data, len, &parse_result);
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int8>*>(col_ptr)->insert_value(num);
        break;
    }
    case TYPE_SMALLINT: {
        int16_t num = StringParser::string_to_int<int16_t>(data, len, &parse_result);
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int16>*>(col_ptr)->insert_value(num);
        break;
    }
    case TYPE_INT: {
        int32_t num = StringParser::string_to_int<int32_t>(data, len, &parse_result);
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int32>*>(col_ptr)->insert_value(num);
        break;
    }
    case TYPE_BIGINT: {
        int64_t num = StringParser::string_to_int<int64_t>(data, len, &parse_result);
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int64>*>(col_ptr)->insert_value(num);
        break;
    }
    case TYPE_LARGEINT: {
        __int128 num = StringParser::string_to_int<__int128>(data, len, &parse_result);
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int128>*>(col_ptr)->insert_value(num);
        break;
    }

    case TYPE_FLOAT: {
        float num = StringParser::string_to_float<float>(data, len, &parse_result);
        reinterpret_cast<vectorized::ColumnVector<vectorized::Float32>*>(col_ptr)->insert_value(
                num);
        break;
    }
    case TYPE_DOUBLE: {
        double num = StringParser::string_to_float<double>(data, len, &parse_result);
        reinterpret_cast<vectorized::ColumnVector<vectorized::Float64>*>(col_ptr)->insert_value(
                num);
        break;
    }
    case TYPE_DATE: {
        vectorized::VecDateTimeValue ts_slot;
        if (!ts_slot.from_date_str(data, len)) {
            parse_result = StringParser::PARSE_FAILURE;
            break;
        }
        ts_slot.cast_to_date();
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int64>*>(col_ptr)->insert_data(
                reinterpret_cast<char*>(&ts_slot), 0);
        break;
    }

    case TYPE_DATETIME: {
        vectorized::VecDateTimeValue ts_slot;
        if (!ts_slot.from_date_str(data, len)) {
            parse_result = StringParser::PARSE_FAILURE;
            break;
        }
        ts_slot.to_datetime();
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int64>*>(col_ptr)->insert_data(
                reinterpret_cast<char*>(&ts_slot), 0);
        break;
    }

    case TYPE_DECIMALV2: {
        DecimalV2Value decimal_slot;
        if (decimal_slot.parse_from_str(data, len)) {
            parse_result = StringParser::PARSE_FAILURE;
            break;
        }
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int128>*>(col_ptr)->insert_value(
                decimal_slot.value());
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
            null_map_data[size - 1] = 1;
            nullable_column->get_nested_column().insert_default();
        }
        return false;
    }
    return true;
}

} // namespace doris
