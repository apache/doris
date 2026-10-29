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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/DataTypeNullable.cpp
// and modified by Doris

#include "core/data_type/data_type_nullable.h"

#include <fmt/format.h>
#include <gen_cpp/data.pb.h>
#include <glog/logging.h>
#include <streamvbyte.h>

#include <algorithm>
#include <cstring>
#include <limits>
#include <utility>

#include "agent/be_exec_version_manager.h"
#include "common/cast_set.h"
#include "core/assert_cast.h"
#include "core/column/column.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_nothing.h"
#include "core/field.h"
#include "core/string_buffer.hpp"
#include "core/types.h"

namespace doris {
#include "common/compile_check_begin.h"

namespace {

size_t validate_streamvbyte_null_map(const char* buf, size_t remaining, size_t null_map_size) {
    if (remaining < sizeof(size_t)) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Malformed nullable column: missing null map length");
    }
    size_t encode_size = unaligned_load<size_t>(buf);
    size_t payload_remaining = remaining - sizeof(size_t);
    if (encode_size > payload_remaining || payload_remaining - encode_size < STREAMVBYTE_PADDING) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Malformed nullable column: encoded null map exceeds input");
    }
    if (null_map_size > std::numeric_limits<UInt32>::max()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Malformed nullable column: null map is too large: {}", null_map_size);
    }

    const auto* encoded_data = reinterpret_cast<const uint8_t*>(buf + sizeof(size_t));
    uint32_t value_count = cast_set<UInt32>(upper_int32(null_map_size));
    size_t control_bytes = (static_cast<size_t>(value_count) + 3) / 4;
    if (control_bytes > encode_size) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Malformed nullable column: null map control bytes exceed input");
    }

    // StreamVByte stores one control byte per four integers, and each
    // two-bit code selects one to four data bytes for its integer.
    size_t expected_encode_size = control_bytes;
    size_t remaining_values = value_count;
    for (size_t i = 0; i < control_bytes; ++i) {
        uint8_t control = encoded_data[i];
        size_t values_in_group = std::min<size_t>(remaining_values, 4);
        for (size_t j = 0; j < values_in_group; ++j) {
            size_t value_bytes = ((control >> (j * 2)) & 0x03) + 1;
            if (value_bytes > encode_size - expected_encode_size) {
                throw Exception(ErrorCode::INVALID_ARGUMENT,
                                "Malformed nullable column: null map data exceeds input");
            }
            expected_encode_size += value_bytes;
        }
        remaining_values -= values_in_group;
    }
    if (expected_encode_size != encode_size) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Malformed nullable column: encoded null map size mismatch: {} != {}",
                        expected_encode_size, encode_size);
    }
    return encode_size;
}

} // namespace

DataTypeNullable::DataTypeNullable(const DataTypePtr& nested_data_type_)
        : nested_data_type {nested_data_type_} {
    if (!nested_data_type) {
        throw Exception(ErrorCode::INTERNAL_ERROR, "DataTypeNullable input nested type is nullptr");
    }
}
// binary: const flag | row num | read saved num| <null array> | <values array>
//  <null array>: is_null1 | is_null2 | ...
//  <values array>: value1 | value2 | ...>
int64_t DataTypeNullable::get_uncompressed_serialized_bytes(const IColumn& column,
                                                            int be_exec_version) const {
    if (be_exec_version >= USE_CONST_SERDE) {
        auto size = sizeof(bool) + sizeof(size_t) + sizeof(size_t);
        bool is_const_column = is_column_const(column);
        auto real_need_copy_num = is_const_column ? 1 : column.size();
        const IColumn* data_column = &column;
        if (is_const_column) {
            const auto& const_column = assert_cast<const ColumnConst&>(column);
            data_column = &(const_column.get_data_column());
        }

        const auto mem_size = real_need_copy_num * sizeof(bool);
        if (mem_size <= SERIALIZED_MEM_SIZE_LIMIT) {
            size += mem_size;
        } else {
            // Throw exception if mem_size is large than UINT32_MAX
            size = size + sizeof(size_t) +
                   std::max(mem_size, streamvbyte_max_compressedbytes(
                                              cast_set<UInt32>(upper_int32(mem_size))));
        }
        const auto& col = assert_cast<const ColumnNullable&>(*data_column);
        size = size + nested_data_type->get_uncompressed_serialized_bytes(col.get_nested_column(),
                                                                          be_exec_version);
        return size;
    } else {
        size_t ret = 0;
        if (size_t size = sizeof(bool) * column.size(); size <= SERIALIZED_MEM_SIZE_LIMIT) {
            ret += size + sizeof(uint32_t);
        } else {
            // Throw exception if mem_size is large than UINT32_MAX
            ret += (sizeof(uint32_t) + sizeof(size_t) +
                    std::max(size,
                             streamvbyte_max_compressedbytes(cast_set<UInt32>(upper_int32(size)))));
        }
        ret += nested_data_type->get_uncompressed_serialized_bytes(
                assert_cast<const ColumnNullable&>(*column.convert_to_full_column_if_const())
                        .get_nested_column(),
                be_exec_version);
        return ret;
    }
}

char* DataTypeNullable::serialize(const IColumn& column, char* buf, int be_exec_version) const {
    if (be_exec_version >= USE_CONST_SERDE) {
        const auto* data_column = &column;
        size_t real_need_copy_num = 0;
        buf = serialize_const_flag_and_row_num(&data_column, buf, &real_need_copy_num);

        // mem_size = real_row_num * sizeof(T)
        const auto mem_size = real_need_copy_num * sizeof(bool);
        const auto& col = assert_cast<const ColumnNullable&>(*data_column);
        // null flags
        if (mem_size <= SERIALIZED_MEM_SIZE_LIMIT) {
            memcpy(buf, col.get_null_map_data().data(), mem_size);
            buf += mem_size;
        } else {
            // Throw exception if mem_size is large than UINT32_MAX
            auto encode_size = streamvbyte_encode(
                    reinterpret_cast<const uint32_t*>(col.get_null_map_data().data()),
                    cast_set<UInt32>(upper_int32(mem_size)), (uint8_t*)(buf + sizeof(size_t)));
            unaligned_store<size_t>(buf, encode_size);
            buf += (sizeof(size_t) + encode_size);
        }
        // data values
        return nested_data_type->serialize(col.get_nested_column(), buf, be_exec_version);
    } else {
        auto ptr = column.convert_to_full_column_if_const();
        const auto& col = assert_cast<const ColumnNullable&>(*ptr.get());

        // row num
        auto mem_size = col.size() * sizeof(bool);
        unaligned_store<uint32_t>(buf, static_cast<UInt32>(mem_size));
        buf += sizeof(uint32_t);
        // null flags
        if (mem_size <= SERIALIZED_MEM_SIZE_LIMIT) {
            memcpy(buf, col.get_null_map_data().data(), mem_size);
            buf += mem_size;
        } else {
            // Throw exception if mem_size is large than UINT32_MAX
            auto encode_size = streamvbyte_encode(
                    reinterpret_cast<const uint32_t*>(col.get_null_map_data().data()),
                    cast_set<UInt32>(upper_int32(mem_size)), (uint8_t*)(buf + sizeof(size_t)));
            *reinterpret_cast<size_t*>(buf) = encode_size;
            buf += (sizeof(size_t) + encode_size);
        }
        // data values
        return nested_data_type->serialize(col.get_nested_column(), buf, be_exec_version);
    }
}

const char* DataTypeNullable::deserialize(const char* buf, MutableColumnPtr* column,
                                          int be_exec_version) const {
    return deserialize(buf, nullptr, column, be_exec_version);
}

const char* DataTypeNullable::deserialize(const char* buf, const char* end,
                                          MutableColumnPtr* column, int be_exec_version) const {
    if (end != nullptr && end < buf) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Malformed nullable column: invalid input range");
    }

    if (be_exec_version >= USE_CONST_SERDE) {
        constexpr size_t header_size = sizeof(bool) + 2 * sizeof(size_t);
        if (end != nullptr) {
            if (static_cast<size_t>(end - buf) < header_size) {
                throw Exception(ErrorCode::INVALID_ARGUMENT,
                                "Malformed nullable column: missing serialized header");
            }
            uint8_t is_const_flag = unaligned_load<uint8_t>(buf);
            if (is_const_flag > 1) {
                throw Exception(ErrorCode::INVALID_ARGUMENT,
                                "Malformed nullable column: invalid const flag {}", is_const_flag);
            }
            bool is_const = is_const_flag != 0;
            size_t row_num = unaligned_load<size_t>(buf + sizeof(bool));
            size_t saved_num = unaligned_load<size_t>(buf + sizeof(bool) + sizeof(size_t));
            if (saved_num != (is_const ? 1 : row_num)) {
                throw Exception(
                        ErrorCode::INVALID_ARGUMENT,
                        "Malformed nullable column: inconsistent row count {}, saved count {}",
                        row_num, saved_num);
            }
        }

        auto* origin_column = column->get();
        size_t real_have_saved_num = 0;
        buf = deserialize_const_flag_and_row_num(buf, column, &real_have_saved_num);

        auto* col = assert_cast<ColumnNullable*>(origin_column);
        const auto& const_col = *col;
        auto nested = std::move(*const_col.get_nested_column_ptr()).mutate();
        auto null_map = std::move(*const_col.get_null_map_column_ptr()).mutate();
        auto& null_map_data = assert_cast<ColumnUInt8&>(*null_map).get_data();
        // null flags
        auto mem_size = real_have_saved_num * sizeof(bool);
        size_t encode_size = 0;
        if (end != nullptr) {
            size_t remaining = static_cast<size_t>(end - buf);
            if (mem_size <= SERIALIZED_MEM_SIZE_LIMIT) {
                if (mem_size > remaining) {
                    throw Exception(ErrorCode::INVALID_ARGUMENT,
                                    "Malformed nullable column: truncated null map");
                }
            } else {
                encode_size = validate_streamvbyte_null_map(buf, remaining, mem_size);
            }
        }
        null_map_data.resize(real_have_saved_num);
        if (mem_size <= SERIALIZED_MEM_SIZE_LIMIT) {
            memcpy(null_map_data.data(), buf, mem_size);
            buf += mem_size;
        } else {
            if (end == nullptr) {
                encode_size = unaligned_load<size_t>(buf);
            }
            buf += sizeof(size_t);
            // Throw exception if mem_size is large than UINT32_MAX
            uint32_t value_count = cast_set<UInt32>(upper_int32(mem_size));
            size_t decoded_bytes = streamvbyte_decode(
                    (const uint8_t*)buf, (uint32_t*)(null_map_data.data()), value_count);
            if (end != nullptr && decoded_bytes != encode_size) {
                throw Exception(
                        ErrorCode::INVALID_ARGUMENT,
                        "Malformed nullable column: decoded null map size mismatch: {} != {}",
                        decoded_bytes, encode_size);
            }
            buf += encode_size;
        }
        // column data values
        buf = nested_data_type->deserialize(buf, end, &nested, be_exec_version);
        col->replace_columns(std::move(nested), std::move(null_map));
        return buf;
    } else {
        if (end != nullptr && static_cast<size_t>(end - buf) < sizeof(uint32_t)) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Malformed nullable column: missing serialized row count");
        }
        auto* col = assert_cast<ColumnNullable*>(column->get());
        const auto& const_col = *col;
        auto nested = std::move(*const_col.get_nested_column_ptr()).mutate();
        auto null_map = std::move(*const_col.get_null_map_column_ptr()).mutate();
        auto& null_map_data = assert_cast<ColumnUInt8&>(*null_map).get_data();
        // row num
        uint32_t mem_size = unaligned_load<uint32_t>(buf);
        buf += sizeof(uint32_t);
        size_t encode_size = 0;
        if (end != nullptr) {
            size_t remaining = static_cast<size_t>(end - buf);
            if (mem_size <= SERIALIZED_MEM_SIZE_LIMIT && mem_size > remaining) {
                throw Exception(ErrorCode::INVALID_ARGUMENT,
                                "Malformed nullable column: truncated null map");
            }
            if (mem_size > SERIALIZED_MEM_SIZE_LIMIT) {
                encode_size = validate_streamvbyte_null_map(buf, remaining, mem_size);
            }
        }
        // null flags
        null_map_data.resize(mem_size / sizeof(bool));
        if (mem_size <= SERIALIZED_MEM_SIZE_LIMIT) {
            memcpy(null_map_data.data(), buf, mem_size);
            buf += mem_size;
        } else {
            if (end == nullptr) {
                encode_size = *reinterpret_cast<const size_t*>(buf);
            }
            buf += sizeof(size_t);
            // Throw exception if mem_size is large than UINT32_MAX
            uint32_t value_count = cast_set<UInt32>(upper_int32(mem_size));
            size_t decoded_bytes = streamvbyte_decode(
                    (const uint8_t*)buf, (uint32_t*)(null_map_data.data()), value_count);
            if (end != nullptr && decoded_bytes != encode_size) {
                throw Exception(
                        ErrorCode::INVALID_ARGUMENT,
                        "Malformed nullable column: decoded null map size mismatch: {} != {}",
                        decoded_bytes, encode_size);
            }
            buf += encode_size;
        }
        // data values
        buf = nested_data_type->deserialize(buf, end, &nested, be_exec_version);
        col->replace_columns(std::move(nested), std::move(null_map));
        return buf;
    }
}

void DataTypeNullable::to_pb_column_meta(PColumnMeta* col_meta) const {
    col_meta->set_is_nullable(true);
    get_nested_type()->to_pb_column_meta(col_meta);
}

MutableColumnPtr DataTypeNullable::create_column() const {
    return ColumnNullable::create(nested_data_type->create_column(), ColumnUInt8::create());
}

Status DataTypeNullable::check_column(const IColumn& column) const {
    const auto* column_nullable = DORIS_TRY(check_column_nested_type<ColumnNullable>(column));
    return nested_data_type->check_column(column_nullable->get_nested_column());
}

bool DataTypeNullable::equals(const IDataType& rhs) const {
    return rhs.is_nullable() &&
           nested_data_type->equals(*static_cast<const DataTypeNullable&>(rhs).nested_data_type);
}

FieldWithDataType DataTypeNullable::get_field_with_data_type(const IColumn& column,
                                                             size_t row_num) const {
    const auto& nullable_column =
            assert_cast<const ColumnNullable&, TypeCheckOnRelease::DISABLE>(column);
    if (nullable_column.is_null_at(row_num)) {
        return FieldWithDataType {.field = Field()};
    }
    return nested_data_type->get_field_with_data_type(nullable_column.get_nested_column(), row_num);
}

DataTypePtr make_nullable(const DataTypePtr& type) {
    if (type->is_nullable()) {
        return type;
    }
    return std::make_shared<DataTypeNullable>(type);
}

DataTypes make_nullable(const DataTypes& types) {
    DataTypes nullable_types;
    for (const auto& type : types) {
        nullable_types.push_back(make_nullable(type));
    }
    return nullable_types;
}

DataTypePtr remove_nullable(const DataTypePtr& type) {
    if (type->is_nullable()) {
        return assert_cast<const DataTypeNullable*>(type.get())->get_nested_type();
    }
    return type;
}

DataTypes remove_nullable(const DataTypes& types) {
    DataTypes no_null_types;
    for (const auto& type : types) {
        no_null_types.push_back(remove_nullable(type));
    }
    return no_null_types;
}

bool have_nullable(const DataTypes& types) {
    return std::ranges::any_of(types, [](const DataTypePtr& type) { return type->is_nullable(); });
}

} // namespace doris
