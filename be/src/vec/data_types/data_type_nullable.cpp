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

#include "common/logging.h"
#include "gen_cpp/data.pb.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/assert_cast.h"
#include "vec/common/typeid_cast.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type_nothing.h"

namespace doris::vectorized {

DataTypeNullable::DataTypeNullable(const DataTypePtr& nested_data_type_)
        : nested_data_type{nested_data_type_} {
    if (!nested_data_type->can_be_inside_nullable()) {
        LOG(FATAL) << fmt::format("Nested type {} cannot be inside Nullable type",
                                  nested_data_type->get_name());
    }
}

bool DataTypeNullable::only_null() const {
    return typeid_cast<const DataTypeNothing*>(nested_data_type.get());
}

std::string DataTypeNullable::to_string(const IColumn& column, size_t row_num) const {
    const ColumnNullable& col =
            assert_cast<const ColumnNullable&>(*column.convert_to_full_column_if_const().get());

    if (col.is_null_at(row_num)) {
        return "\\N";
    } else {
        return nested_data_type->to_string(col.get_nested_column(), row_num);
    }
}

// binary: row num | <null array> | <values array>
//  <null array>: is_null1 | is_null2 | ...
//  <values array>: value1 | value2 | ...>
int64_t DataTypeNullable::get_uncompressed_serialized_bytes(const IColumn& column) const {
    int64_t size = sizeof(uint32_t);
    size += sizeof(bool) * column.size();
    size += nested_data_type->get_uncompressed_serialized_bytes(assert_cast<const ColumnNullable&>(
            *column.convert_to_full_column_if_const()).get_nested_column());
    return size;
}

char* DataTypeNullable::serialize(const IColumn& column, char* buf) const {
    auto ptr = column.convert_to_full_column_if_const();
    const ColumnNullable& col = assert_cast<const ColumnNullable&>(*ptr.get());

    // row num
    *reinterpret_cast<uint32_t*>(buf) = column.size();
    buf += sizeof(uint32_t);
    // null flags
    memcpy(buf, col.get_null_map_data().data(), column.size() * sizeof(bool));
    buf += column.size() * sizeof(bool);
    // data values
    return nested_data_type->serialize(col.get_nested_column(), buf);
}

const char* DataTypeNullable::deserialize(const char* buf, IColumn* column) const {
    ColumnNullable* col = assert_cast<ColumnNullable*>(column);
    // row num
    uint32_t row_num = *reinterpret_cast<const uint32_t*>(buf);
    buf += sizeof(uint32_t);
    // null flags
    col->get_null_map_data().resize(row_num);
    memcpy(col->get_null_map_data().data(), buf, row_num * sizeof(bool));
    buf += row_num * sizeof(bool);
    // data values
    IColumn& nested = col->get_nested_column();
    return nested_data_type->deserialize(buf, &nested);
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
    if (type->is_nullable()) return type;
    return std::make_shared<DataTypeNullable>(type);
}

DataTypePtr remove_nullable(const DataTypePtr& type) {
    if (type->is_nullable()) return static_cast<const DataTypeNullable&>(*type).get_nested_type();
    return type;
}

} // namespace doris::vectorized
