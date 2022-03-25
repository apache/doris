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

#include "gen_cpp/data.pb.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_string.h"
#include "vec/common/assert_cast.h"
#include "vec/core/field.h"
#include "vec/io/io_helper.h"

#ifdef __SSE2__
#include <emmintrin.h>
#endif

namespace doris::vectorized {

template <typename Reader>
static inline void read(IColumn& column, Reader&& reader) {
    ColumnString& column_string = assert_cast<ColumnString&>(column);
    ColumnString::Chars& data = column_string.get_chars();
    ColumnString::Offsets& offsets = column_string.get_offsets();
    size_t old_chars_size = data.size();
    size_t old_offsets_size = offsets.size();
    try {
        reader(data);
        data.push_back(0);
        offsets.push_back(data.size());
    } catch (...) {
        offsets.resize_assume_reserved(old_offsets_size);
        data.resize_assume_reserved(old_chars_size);
        throw;
    }
}

std::string DataTypeString::to_string(const IColumn& column, size_t row_num) const {
    const StringRef& s =
            assert_cast<const ColumnString&>(*column.convert_to_full_column_if_const().get())
                    .get_data_at(row_num);
    return s.to_string();
}

void DataTypeString::to_string(const class doris::vectorized::IColumn & column, size_t row_num,
        class doris::vectorized::BufferWritable & ostr) const {
    const StringRef& s =
            assert_cast<const ColumnString&>(*column.convert_to_full_column_if_const().get())
                    .get_data_at(row_num);
    ostr.write(s.data, s.size);
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
int64_t DataTypeString::get_uncompressed_serialized_bytes(const IColumn& column) const {
    auto ptr = column.convert_to_full_column_if_const();
    const auto& data_column = assert_cast<const ColumnString&>(*ptr.get());
    return sizeof(uint32_t) * (column.size() + 1) + sizeof(uint64_t) + data_column.get_chars().size();
}

char* DataTypeString::serialize(const IColumn& column, char* buf) const {
    auto ptr = column.convert_to_full_column_if_const();
    const auto& data_column = assert_cast<const ColumnString&>(*ptr.get());

    // row num
    *reinterpret_cast<uint32_t*>(buf) = column.size();
    buf += sizeof(uint32_t);
    // offsets
    memcpy(buf, data_column.get_offsets().data(), column.size() * sizeof(uint32_t));
    buf += column.size() * sizeof(uint32_t);
    // total length
    uint64_t value_len = data_column.get_chars().size();
    *reinterpret_cast<uint64_t*>(buf) = value_len;
    buf += sizeof(uint64_t);
    // values
    memcpy(buf, data_column.get_chars().data(), value_len);
    buf += value_len;

    return buf;
}

const char* DataTypeString::deserialize(const char* buf, IColumn* column) const {
    ColumnString* column_string = assert_cast<ColumnString*>(column);
    ColumnString::Chars& data = column_string->get_chars();
    ColumnString::Offsets& offsets = column_string->get_offsets();

    // row num
    uint32_t row_num = *reinterpret_cast<const uint32_t*>(buf);
    buf += sizeof(uint32_t);
    // offsets
    offsets.resize(row_num);
    memcpy(offsets.data(), buf, sizeof(uint32_t) * row_num);
    buf += sizeof(uint32_t) * row_num;
    // total length
    uint64_t value_len = *reinterpret_cast<const uint64_t*>(buf); 
    buf += sizeof(uint64_t);
    // values
    data.resize(value_len);
    memcpy(data.data(), buf, value_len);
    buf += value_len;

    return buf;
}

} // namespace doris::vectorized
