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

#include "vec/data_types/data_type_nullable.h"

#include <fmt/format.h>
#include <gen_cpp/data.pb.h>
#include <glog/logging.h>
#include <streamvbyte.h>

#include <algorithm>
#include <cstring>
#include <utility>

#include "agent/be_exec_version_manager.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_buffer.hpp"
#include "vec/core/field.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nothing.h"
#include "vec/io/reader_buffer.h"

namespace doris::vectorized {

DataTypeNullable::DataTypeNullable(const DataTypePtr& nested_data_type_)
        : nested_data_type {nested_data_type_} {
    if (!nested_data_type) {
        throw Exception(ErrorCode::INTERNAL_ERROR, "DataTypeNullable input nested type is nullptr");
    }
}

std::string DataTypeNullable::to_string(const IColumn& column, size_t row_num) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    const auto& col_null = assert_cast<const ColumnNullable&>(*ptr);
    if (col_null.is_null_at(row_num)) {
        return "NULL";
    } else {
        return get_nested_type()->to_string(col_null.get_nested_column(), row_num);
    }
}

void DataTypeNullable::to_string(const IColumn& column, size_t row_num,
                                 BufferWritable& ostr) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    const auto& col_null = assert_cast<const ColumnNullable&>(*ptr);
    if (col_null.is_null_at(row_num)) {
        ostr.write("NULL", 4);
    } else {
        get_nested_type()->to_string(col_null.get_nested_column(), row_num, ostr);
    }
}

Status DataTypeNullable::from_string(ReadBuffer& rb, IColumn* column) const {
    auto* null_column = assert_cast<ColumnNullable*>(column);
    if (rb.count() == 4 && *(rb.position()) == 'N' && *(rb.position() + 1) == 'U' &&
        *(rb.position() + 2) == 'L' && *(rb.position() + 3) == 'L') {
        null_column->insert_data(nullptr, 0);
        return Status::OK();
    }
    auto st = nested_data_type->from_string(rb, &(null_column->get_nested_column()));
    if (!st.ok()) {
        // fill null if fail
        null_column->insert_data(nullptr, 0); // 0 is meaningless here
        return Status::OK();
    }
    // fill not null if succ
    null_column->get_null_map_data().push_back(0);
    return Status::OK();
}

// binary: row num | <null array> | <values array>
//  <null array>: is_null1 | is_null2 | ...
//  <values array>: value1 | value2 | ...>
int64_t DataTypeNullable::get_uncompressed_serialized_bytes(const IColumn& column,
                                                            int be_exec_version) const {
    if (be_exec_version >= USE_NEW_SERDE) {
        size_t ret = 0;
        if (size_t size = sizeof(bool) * column.size(); size <= SERIALIZED_MEM_SIZE_LIMIT) {
            ret += size + sizeof(uint32_t);
        } else {
            ret += (sizeof(uint32_t) + sizeof(size_t) +
                    std::max(size, streamvbyte_max_compressedbytes(upper_int32(size))));
        }
        ret += nested_data_type->get_uncompressed_serialized_bytes(
                assert_cast<const ColumnNullable&>(*column.convert_to_full_column_if_const())
                        .get_nested_column(),
                be_exec_version);
        return ret;
    } else {
        int64_t size = sizeof(uint32_t);
        size += sizeof(bool) * column.size();
        size += nested_data_type->get_uncompressed_serialized_bytes(
                assert_cast<const ColumnNullable&>(*column.convert_to_full_column_if_const())
                        .get_nested_column(),
                be_exec_version);
        return size;
    }
}

char* DataTypeNullable::serialize(const IColumn& column, char* buf, int be_exec_version) const {
    if (be_exec_version >= USE_NEW_SERDE) {
        auto ptr = column.convert_to_full_column_if_const();
        const auto& col = assert_cast<const ColumnNullable&>(*ptr.get());

        // row num
        auto mem_size = col.size() * sizeof(bool);
        *reinterpret_cast<uint32_t*>(buf) = mem_size;
        buf += sizeof(uint32_t);
        // null flags
        if (mem_size <= SERIALIZED_MEM_SIZE_LIMIT) {
            memcpy(buf, col.get_null_map_data().data(), mem_size);
            buf += mem_size;
        } else {
            auto encode_size = streamvbyte_encode(
                    reinterpret_cast<const uint32_t*>(col.get_null_map_data().data()),
                    upper_int32(mem_size), (uint8_t*)(buf + sizeof(size_t)));
            *reinterpret_cast<size_t*>(buf) = encode_size;
            buf += (sizeof(size_t) + encode_size);
        }
        // data values
        return nested_data_type->serialize(col.get_nested_column(), buf, be_exec_version);
    } else {
        auto ptr = column.convert_to_full_column_if_const();
        const auto& col = assert_cast<const ColumnNullable&>(*ptr.get());

        // row num
        *reinterpret_cast<uint32_t*>(buf) = column.size();
        buf += sizeof(uint32_t);
        // null flags
        memcpy(buf, col.get_null_map_data().data(), column.size() * sizeof(bool));
        buf += column.size() * sizeof(bool);
        // data values
        return nested_data_type->serialize(col.get_nested_column(), buf, be_exec_version);
    }
}

const char* DataTypeNullable::deserialize(const char* buf, IColumn* column,
                                          int be_exec_version) const {
    if (be_exec_version >= USE_NEW_SERDE) {
        auto* col = assert_cast<ColumnNullable*>(column);
        // row num
        uint32_t mem_size = *reinterpret_cast<const uint32_t*>(buf);
        buf += sizeof(uint32_t);
        // null flags
        col->get_null_map_data().resize(mem_size / sizeof(bool));
        if (mem_size <= SERIALIZED_MEM_SIZE_LIMIT) {
            memcpy(col->get_null_map_data().data(), buf, mem_size);
            buf += mem_size;
        } else {
            size_t encode_size = *reinterpret_cast<const size_t*>(buf);
            buf += sizeof(size_t);
            streamvbyte_decode((const uint8_t*)buf, (uint32_t*)(col->get_null_map_data().data()),
                               upper_int32(mem_size));
            buf += encode_size;
        }
        // data values
        IColumn& nested = col->get_nested_column();
        return nested_data_type->deserialize(buf, &nested, be_exec_version);
    } else {
        auto* col = assert_cast<ColumnNullable*>(column);
        // row num
        uint32_t row_num = *reinterpret_cast<const uint32_t*>(buf);
        buf += sizeof(uint32_t);
        // null flags
        col->get_null_map_data().resize(row_num);
        memcpy(col->get_null_map_data().data(), buf, row_num * sizeof(bool));
        buf += row_num * sizeof(bool);
        // data values
        IColumn& nested = col->get_nested_column();
        return nested_data_type->deserialize(buf, &nested, be_exec_version);
    }
}

void DataTypeNullable::to_pb_column_meta(PColumnMeta* col_meta) const {
    col_meta->set_is_nullable(true);
    get_nested_type()->to_pb_column_meta(col_meta);
}

MutableColumnPtr DataTypeNullable::create_column() const {
    return ColumnNullable::create(nested_data_type->create_column(), ColumnUInt8::create());
}

Field DataTypeNullable::get_default() const {
    return Null();
}

size_t DataTypeNullable::get_size_of_value_in_memory() const {
    LOG(FATAL) << fmt::format("Value of type {} in memory is not of fixed size.", get_name());
    return 0;
}

bool DataTypeNullable::equals(const IDataType& rhs) const {
    return rhs.is_nullable() &&
           nested_data_type->equals(*static_cast<const DataTypeNullable&>(rhs).nested_data_type);
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
    return std::any_of(types.begin(), types.end(),
                       [](const DataTypePtr& type) { return type->is_nullable(); });
}

} // namespace doris::vectorized
