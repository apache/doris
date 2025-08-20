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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/DataTypeString.cpp
// and modified by Doris

#include "vec/data_types/data_type_string.h"

#include <lz4/lz4.h>
#include <streamvbyte.h>

#include <cstddef>
#include <cstdint>
#include <cstring>

#include "agent/be_exec_version_manager.h"
#include "common/cast_set.h"
#include "common/exception.h"
#include "common/status.h"
#include "runtime/primitive_type.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_string.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_buffer.hpp"
#include "vec/common/string_ref.h"
#include "vec/core/field.h"
#include "vec/core/types.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
std::string DataTypeString::to_string(const IColumn& column, size_t row_num) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    const auto& value = assert_cast<const ColumnString&>(*ptr).get_data_at(row_num);
    return value.to_string();
}

void DataTypeString::to_string(const class doris::vectorized::IColumn& column, size_t row_num,
                               class doris::vectorized::BufferWritable& ostr) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    const auto& value = assert_cast<const ColumnString&>(*ptr).get_data_at(row_num);
    ostr.write(value.data, value.size);
}

Field DataTypeString::get_default() const {
    return Field::create_field<TYPE_STRING>(String());
}

MutableColumnPtr DataTypeString::create_column() const {
    return ColumnString::create();
}

Status DataTypeString::check_column(const IColumn& column) const {
    if (column.is_column_string64()) {
        return Status::OK();
    }
    return check_column_non_nested_type<ColumnString>(column);
}

bool DataTypeString::equals(const IDataType& rhs) const {
    return typeid(rhs) == typeid(*this);
}

// binary: const flag | row num | read saved num | offset | chars
// offset: {offset1 | offset2 ...} or {encode_size | offset1 |offset2 ...}
// chars : {value_length | <value1> | <value2 ...} or {value_length | encode_size | <value1> | <value2 ...}
int64_t DataTypeString::get_uncompressed_serialized_bytes(const IColumn& column,
                                                          int be_exec_version) const {
    if (be_exec_version >= USE_CONST_SERDE) {
        int64_t size = sizeof(bool) + sizeof(size_t) + sizeof(size_t);
        bool is_const_column = is_column_const(column);
        const IColumn* string_column = &column;
        if (is_const_column) {
            const auto& const_column = assert_cast<const ColumnConst&>(column);
            string_column = &(const_column.get_data_column());
        }
        const auto& data_column = assert_cast<const ColumnString&>(*string_column);

        auto real_need_copy_num = is_const_column ? 1 : data_column.size();
        auto offsets_size = real_need_copy_num * sizeof(IColumn::Offset);
        if (offsets_size <= SERIALIZED_MEM_SIZE_LIMIT) {
            size += offsets_size;
        } else {
            // Throw exception if offsets_size is large than UINT32_MAX
            size += sizeof(size_t) +
                    std::max(offsets_size, streamvbyte_max_compressedbytes(
                                                   cast_set<UInt32>(upper_int32(offsets_size))));
        }
        size += sizeof(size_t);
        if (size_t bytes = data_column.get_chars().size(); bytes <= SERIALIZED_MEM_SIZE_LIMIT) {
            size += bytes;
        } else {
            if (bytes > LZ4_MAX_INPUT_SIZE) {
                throw Exception(ErrorCode::BUFFER_OVERFLOW,
                                "LZ4_compressBound meet invalid input size, input_size={}, "
                                "LZ4_MAX_INPUT_SIZE={}",
                                bytes, LZ4_MAX_INPUT_SIZE);
            }
            size += sizeof(size_t) +
                    std::max(bytes, (size_t)LZ4_compressBound(cast_set<UInt32>(bytes)));
        }
        return size;
    } else {
        auto ptr = column.convert_to_full_column_if_const();
        const auto& data_column = assert_cast<const ColumnString&>(*ptr.get());
        int64_t size = sizeof(uint32_t) + sizeof(uint64_t);
        if (auto offsets_size = data_column.size() * sizeof(IColumn::Offset);
            offsets_size <= SERIALIZED_MEM_SIZE_LIMIT) {
            size += offsets_size;
        } else {
            // Throw exception if offsets_size is large than UINT32_MAX
            size += sizeof(size_t) +
                    std::max(offsets_size, streamvbyte_max_compressedbytes(
                                                   cast_set<UInt32>(upper_int32(offsets_size))));
        }

        if (auto bytes = data_column.get_chars().size(); bytes <= SERIALIZED_MEM_SIZE_LIMIT) {
            size += bytes;
        } else {
            // Throw exception if bytes is large than UINT32_MAX
            size += sizeof(size_t) +
                    std::max(bytes, (size_t)LZ4_compressBound(cast_set<UInt32>(bytes)));
        }
        return size;
    }
}

char* DataTypeString::serialize(const IColumn& column, char* buf, int be_exec_version) const {
    if (be_exec_version >= USE_CONST_SERDE) {
        const auto* data_column = &column;
        size_t real_need_copy_num = 0;
        buf = serialize_const_flag_and_row_num(&data_column, buf, &real_need_copy_num);

        // mem_size = real_row_num * sizeof(IColumn::Offset)
        size_t mem_size = real_need_copy_num * sizeof(IColumn::Offset);
        const auto& string_column = assert_cast<const ColumnString&>(*data_column);
        // offsets
        if (mem_size <= SERIALIZED_MEM_SIZE_LIMIT) {
            memcpy(buf, string_column.get_offsets().data(), mem_size);
            buf += mem_size;
        } else {
            // Throw exception if mem_size is large than UINT32_MAX
            auto encode_size = streamvbyte_encode(
                    reinterpret_cast<const uint32_t*>(string_column.get_offsets().data()),
                    cast_set<uint32_t>(upper_int32(mem_size)), (uint8_t*)(buf + sizeof(size_t)));
            unaligned_store<size_t>(buf, encode_size);
            buf += (sizeof(size_t) + encode_size);
        }

        // values
        auto value_len = string_column.get_chars().size();
        unaligned_store<size_t>(buf, value_len);
        buf += sizeof(size_t);
        if (value_len <= SERIALIZED_MEM_SIZE_LIMIT) {
            memcpy(buf, string_column.get_chars().data(), value_len);
            buf += value_len;
        } else {
            auto encode_size = LZ4_compress_fast(string_column.get_chars().raw_data(),
                                                 (buf + sizeof(size_t)), cast_set<Int32>(value_len),
                                                 LZ4_compressBound(cast_set<Int32>(value_len)), 1);
            unaligned_store<size_t>(buf, encode_size);
            buf += (sizeof(size_t) + encode_size);
        }
        return buf;
    } else {
        auto ptr = column.convert_to_full_column_if_const();
        const auto& data_column = assert_cast<const ColumnString&>(*ptr.get());

        // row num
        size_t mem_size = data_column.size() * sizeof(IColumn::Offset);
        unaligned_store<uint32_t>(buf, static_cast<UInt32>(mem_size));
        buf += sizeof(uint32_t);
        // offsets
        if (mem_size <= SERIALIZED_MEM_SIZE_LIMIT) {
            memcpy(buf, data_column.get_offsets().data(), mem_size);
            buf += mem_size;
        } else {
            // Throw exception if mem_size is large than UINT32_MAX
            auto encode_size = streamvbyte_encode(
                    reinterpret_cast<const uint32_t*>(data_column.get_offsets().data()),
                    cast_set<UInt32>(upper_int32(mem_size)), (uint8_t*)(buf + sizeof(size_t)));
            unaligned_store<size_t>(buf, encode_size);
            buf += (sizeof(size_t) + encode_size);
        }

        // values
        uint64_t value_len = data_column.get_chars().size();
        unaligned_store<uint64_t>(buf, value_len);
        buf += sizeof(uint64_t);
        if (value_len <= SERIALIZED_MEM_SIZE_LIMIT) {
            memcpy(buf, data_column.get_chars().data(), value_len);
            buf += value_len;
            return buf;
        }
        auto encode_size = LZ4_compress_fast(data_column.get_chars().raw_data(),
                                             (buf + sizeof(size_t)), cast_set<Int32>(value_len),
                                             LZ4_compressBound(cast_set<Int32>(value_len)), 1);
        unaligned_store<size_t>(buf, encode_size);
        buf += (sizeof(size_t) + encode_size);
        return buf;
    }
}

const char* DataTypeString::deserialize(const char* buf, MutableColumnPtr* column,
                                        int be_exec_version) const {
    if (be_exec_version >= USE_CONST_SERDE) {
        auto* origin_column = column->get();
        size_t real_have_saved_num = 0;
        buf = deserialize_const_flag_and_row_num(buf, column, &real_have_saved_num);

        auto mem_size = real_have_saved_num * sizeof(IColumn::Offset);
        auto* column_string = assert_cast<ColumnString*>(origin_column);
        ColumnString::Chars& data = column_string->get_chars();
        ColumnString::Offsets& offsets = column_string->get_offsets();
        offsets.resize(real_have_saved_num);

        // offsets
        if (mem_size <= SERIALIZED_MEM_SIZE_LIMIT) {
            memcpy(offsets.data(), buf, mem_size);
            buf += mem_size;
        } else {
            size_t encode_size = unaligned_load<size_t>(buf);
            buf += sizeof(size_t);
            streamvbyte_decode((const uint8_t*)buf, (uint32_t*)(offsets.data()),
                               cast_set<UInt32>(upper_int32(mem_size)));
            buf += encode_size;
        }

        // total length
        size_t value_len = unaligned_load<size_t>(buf);
        buf += sizeof(size_t);
        data.resize(value_len);

        // values
        if (value_len <= SERIALIZED_MEM_SIZE_LIMIT) {
            memcpy(data.data(), buf, value_len);
            buf += value_len;
        } else {
            size_t encode_size = unaligned_load<size_t>(buf);
            buf += sizeof(size_t);
            LZ4_decompress_safe(buf, reinterpret_cast<char*>(data.data()),
                                cast_set<Int32>(encode_size), cast_set<Int32>(value_len));
            buf += encode_size;
        }
        return buf;
    } else {
        auto* column_string = assert_cast<ColumnString*>(column->get());
        ColumnString::Chars& data = column_string->get_chars();
        ColumnString::Offsets& offsets = column_string->get_offsets();

        uint32_t mem_size = unaligned_load<uint32_t>(buf);
        buf += sizeof(uint32_t);
        offsets.resize(mem_size / sizeof(IColumn::Offset));
        // offsets
        if (mem_size <= SERIALIZED_MEM_SIZE_LIMIT) {
            memcpy(offsets.data(), buf, mem_size);
            buf += mem_size;
        } else {
            size_t encode_size = unaligned_load<size_t>(buf);
            buf += sizeof(size_t);
            streamvbyte_decode((const uint8_t*)buf, (uint32_t*)(offsets.data()),
                               cast_set<UInt32>(upper_int32(mem_size)));
            buf += encode_size;
        }
        // total length
        uint64_t value_len = unaligned_load<uint64_t>(buf);
        buf += sizeof(uint64_t);
        data.resize(value_len);

        // values
        if (value_len <= SERIALIZED_MEM_SIZE_LIMIT) {
            memcpy(data.data(), buf, value_len);
            buf += value_len;
        } else {
            size_t encode_size = unaligned_load<size_t>(buf);
            buf += sizeof(size_t);
            LZ4_decompress_safe(buf, reinterpret_cast<char*>(data.data()),
                                cast_set<int32_t>(encode_size), cast_set<Int32>(value_len));
            buf += encode_size;
        }
        return buf;
    }
}

FieldWithDataType DataTypeString::get_field_with_data_type(const IColumn& column,
                                                           size_t row_num) const {
    const auto& column_data = assert_cast<const ColumnString&, TypeCheckOnRelease::DISABLE>(column);
    return FieldWithDataType {
            .field = Field::create_field<TYPE_STRING>(column_data.get_data_at(row_num).to_string()),
            .base_scalar_type_id = get_primitive_type()};
}

} // namespace doris::vectorized
