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

#include <string.h>

#include <typeinfo>
#include <utility>

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
    auto ptr = column.convert_to_full_column_if_const();
    const auto& data_column = assert_cast<const ColumnString&>(*ptr.get());

    if (be_exec_version == 0) {
        return sizeof(IColumn::Offset) * (column.size() + 1) + sizeof(uint64_t) +
               data_column.get_chars().size() + column.size();
    }

    return sizeof(IColumn::Offset) * (column.size() + 1) + sizeof(uint64_t) +
           data_column.get_chars().size();
}

char* DataTypeString::serialize(const IColumn& column, char* buf, int be_exec_version) const {
    auto ptr = column.convert_to_full_column_if_const();
    const auto& data_column = assert_cast<const ColumnString&>(*ptr.get());

    if (be_exec_version == 0) {
        // row num
        *reinterpret_cast<IColumn::Offset*>(buf) = column.size();
        buf += sizeof(IColumn::Offset);
        // offsets
        for (int i = 0; i < column.size(); i++) {
            *reinterpret_cast<IColumn::Offset*>(buf) = data_column.get_offsets()[i] + i + 1;
            buf += sizeof(IColumn::Offset);
        }
        // total length
        *reinterpret_cast<uint64_t*>(buf) = data_column.get_chars().size() + column.size();
        buf += sizeof(uint64_t);
        // values
        for (int i = 0; i < column.size(); i++) {
            auto data = data_column.get_data_at(i);
            memcpy(buf, data.data, data.size);
            buf += data.size;
            *buf = '\0';
            buf++;
        }
        return buf;
    }

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

const char* DataTypeString::deserialize(const char* buf, IColumn* column,
                                        int be_exec_version) const {
    ColumnString* column_string = assert_cast<ColumnString*>(column);
    ColumnString::Chars& data = column_string->get_chars();
    ColumnString::Offsets& offsets = column_string->get_offsets();

    if (be_exec_version == 0) {
        // row num
        IColumn::Offset row_num = *reinterpret_cast<const IColumn::Offset*>(buf);
        buf += sizeof(IColumn::Offset);
        // offsets
        offsets.resize(row_num);
        for (int i = 0; i < row_num; i++) {
            offsets[i] = *reinterpret_cast<const IColumn::Offset*>(buf) - i - 1;
            buf += sizeof(IColumn::Offset);
        }
        // total length
        uint64_t value_len = *reinterpret_cast<const uint64_t*>(buf);
        buf += sizeof(uint64_t);
        // values
        data.resize(value_len - row_num);
        for (int i = 0; i < row_num; i++) {
            memcpy(data.data() + offsets[i - 1], buf, offsets[i] - offsets[i - 1]);
            buf += offsets[i] - offsets[i - 1] + 1;
        }

        return buf;
    }

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

} // namespace doris::vectorized
