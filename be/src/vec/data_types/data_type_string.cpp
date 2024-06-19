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

#include <cstring>

#include "agent/be_exec_version_manager.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_string.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_buffer.hpp"
#include "vec/common/string_ref.h"
#include "vec/core/field.h"
#include "vec/io/reader_buffer.h"

namespace doris::vectorized {

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

Status DataTypeString::from_string(ReadBuffer& rb, IColumn* column) const {
    auto* column_data = static_cast<ColumnString*>(column);
    column_data->insert_data(rb.position(), rb.count());
    return Status::OK();
}

Field DataTypeString::get_default() const {
    return String();
}

MutableColumnPtr DataTypeString::create_column() const {
    return ColumnString::create();
}

bool DataTypeString::equals(const IDataType& rhs) const {
    return typeid(rhs) == typeid(*this);
}

// binary: <size array> | total length | <value array>
//  <size array> : row num | offset1 |offset2 | ...
//  <value array> : <value1> | <value2 | ...
int64_t DataTypeString::get_uncompressed_serialized_bytes(const IColumn& column,
                                                          int be_exec_version) const {
    if (be_exec_version >= USE_NEW_SERDE) {
        auto ptr = column.convert_to_full_column_if_const();
        const auto& data_column = assert_cast<const ColumnString&>(*ptr.get());
        int64_t size = sizeof(uint32_t) + sizeof(uint64_t);
        if (auto offsets_size = data_column.size() * sizeof(IColumn::Offset);
            offsets_size <= SERIALIZED_MEM_SIZE_LIMIT) {
            size += offsets_size;
        } else {
            size += sizeof(size_t) + std::max(offsets_size, streamvbyte_max_compressedbytes(
                                                                    upper_int32(offsets_size)));
        }

        if (auto bytes = data_column.get_chars().size(); bytes <= SERIALIZED_MEM_SIZE_LIMIT) {
            size += bytes;
        } else {
            size += sizeof(size_t) + std::max(bytes, (size_t)LZ4_compressBound(bytes));
        }
        return size;
    } else {
        auto ptr = column.convert_to_full_column_if_const();
        const auto& data_column = assert_cast<const ColumnString&>(*ptr.get());

        return sizeof(IColumn::Offset) * (column.size() + 1) + sizeof(uint64_t) +
               data_column.get_chars().size();
    }
}

char* DataTypeString::serialize(const IColumn& column, char* buf, int be_exec_version) const {
    if (be_exec_version >= USE_NEW_SERDE) {
        auto ptr = column.convert_to_full_column_if_const();
        const auto& data_column = assert_cast<const ColumnString&>(*ptr.get());

        // row num
        uint32_t mem_size = data_column.size() * sizeof(IColumn::Offset);
        *reinterpret_cast<uint32_t*>(buf) = mem_size;
        buf += sizeof(uint32_t);
        // offsets
        if (mem_size <= SERIALIZED_MEM_SIZE_LIMIT) {
            memcpy(buf, data_column.get_offsets().data(), mem_size);
            buf += mem_size;
        } else {
            auto encode_size = streamvbyte_encode(
                    reinterpret_cast<const uint32_t*>(data_column.get_offsets().data()),
                    upper_int32(mem_size), (uint8_t*)(buf + sizeof(size_t)));
            *reinterpret_cast<size_t*>(buf) = encode_size;
            buf += (sizeof(size_t) + encode_size);
        }

        // values
        uint64_t value_len = data_column.get_chars().size();
        *reinterpret_cast<uint64_t*>(buf) = value_len;
        buf += sizeof(uint64_t);
        if (value_len <= SERIALIZED_MEM_SIZE_LIMIT) {
            memcpy(buf, data_column.get_chars().data(), value_len);
            buf += value_len;
            return buf;
        }
        auto encode_size =
                LZ4_compress_fast(data_column.get_chars().raw_data(), (buf + sizeof(size_t)),
                                  value_len, LZ4_compressBound(value_len), 1);
        *reinterpret_cast<size_t*>(buf) = encode_size;
        buf += (sizeof(size_t) + encode_size);
        return buf;
    } else {
        auto ptr = column.convert_to_full_column_if_const();
        const auto& data_column = assert_cast<const ColumnString&>(*ptr.get());
        // row num
        *reinterpret_cast<IColumn::Offset*>(buf) = column.size();
        buf += sizeof(IColumn::Offset);
        // offsets
        memcpy(buf, data_column.get_offsets().data(), column.size() * sizeof(IColumn::Offset));
        buf += column.size() * sizeof(IColumn::Offset);
        // total length
        uint64_t value_len = data_column.get_chars().size();
        *reinterpret_cast<uint64_t*>(buf) = value_len;
        buf += sizeof(uint64_t);
        // values
        memcpy(buf, data_column.get_chars().data(), value_len);
        buf += value_len;

        return buf;
    }
}

const char* DataTypeString::deserialize(const char* buf, IColumn* column,
                                        int be_exec_version) const {
    if (be_exec_version >= USE_NEW_SERDE) {
        auto* column_string = assert_cast<ColumnString*>(column);
        ColumnString::Chars& data = column_string->get_chars();
        ColumnString::Offsets& offsets = column_string->get_offsets();

        uint32_t mem_size = *reinterpret_cast<const uint32_t*>(buf);
        buf += sizeof(uint32_t);
        offsets.resize(mem_size / sizeof(IColumn::Offset));
        // offsets
        if (mem_size <= SERIALIZED_MEM_SIZE_LIMIT) {
            memcpy(offsets.data(), buf, mem_size);
            buf += mem_size;
        } else {
            size_t encode_size = *reinterpret_cast<const size_t*>(buf);
            buf += sizeof(size_t);
            streamvbyte_decode((const uint8_t*)buf, (uint32_t*)(offsets.data()),
                               upper_int32(mem_size));
            buf += encode_size;
        }
        // total length
        uint64_t value_len = *reinterpret_cast<const uint64_t*>(buf);
        buf += sizeof(uint64_t);
        data.resize(value_len);

        // values
        if (value_len <= SERIALIZED_MEM_SIZE_LIMIT) {
            memcpy(data.data(), buf, value_len);
            buf += value_len;
        } else {
            size_t encode_size = *reinterpret_cast<const size_t*>(buf);
            buf += sizeof(size_t);
            LZ4_decompress_safe(buf, reinterpret_cast<char*>(data.data()), encode_size, value_len);
            buf += encode_size;
        }
        return buf;
    } else {
        auto* column_string = assert_cast<ColumnString*>(column);
        ColumnString::Chars& data = column_string->get_chars();
        ColumnString::Offsets& offsets = column_string->get_offsets();
        // row num
        IColumn::Offset row_num = *reinterpret_cast<const IColumn::Offset*>(buf);
        buf += sizeof(IColumn::Offset);
        // offsets
        offsets.resize(row_num);
        memcpy(offsets.data(), buf, sizeof(IColumn::Offset) * row_num);
        buf += sizeof(IColumn::Offset) * row_num;
        // total length
        uint64_t value_len = *reinterpret_cast<const uint64_t*>(buf);
        buf += sizeof(uint64_t);
        // values
        data.resize(value_len);
        memcpy(data.data(), buf, value_len);
        buf += value_len;

        return buf;
    }
}

} // namespace doris::vectorized
