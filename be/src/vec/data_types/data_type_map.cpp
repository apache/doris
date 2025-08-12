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

#include "data_type_map.h"

#include <ctype.h>
#include <gen_cpp/data.pb.h>
#include <glog/logging.h>
#include <string.h>

#include <string>
#include <typeinfo>

#include "agent/be_exec_version_manager.h"
#include "common/status.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_buffer.hpp"
#include "vec/common/string_ref.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris::vectorized {

DataTypeMap::DataTypeMap(const DataTypePtr& key_type_, const DataTypePtr& value_type_) {
    key_type = key_type_;
    value_type = value_type_;
}

Field DataTypeMap::get_default() const {
    Map m;
    Array key, val;
    key.push_back(key_type->get_default());
    val.push_back(value_type->get_default());
    m.push_back(Field::create_field<TYPE_ARRAY>(key));
    m.push_back(Field::create_field<TYPE_ARRAY>(val));
    return Field::create_field<TYPE_MAP>(m);
};

std::string DataTypeMap::to_string(const IColumn& column, size_t row_num) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    const ColumnMap& map_column = assert_cast<const ColumnMap&>(*ptr);
    const ColumnArray::Offsets64& offsets = map_column.get_offsets();

    size_t offset = offsets[row_num - 1];
    size_t next_offset = offsets[row_num];

    const IColumn& nested_keys_column = map_column.get_keys();
    const IColumn& nested_values_column = map_column.get_values();

    std::string str;
    str += "{";
    for (size_t i = offset; i < next_offset; ++i) {
        if (i != offset) {
            str += ", ";
        }
        if (nested_keys_column.is_null_at(i)) {
            str += "null";
        } else if (is_string_type(key_type->get_primitive_type())) {
            str += "\"" + key_type->to_string(nested_keys_column, i) + "\"";
        } else {
            str += key_type->to_string(nested_keys_column, i);
        }
        str += ":";
        if (nested_values_column.is_null_at(i)) {
            str += "null";
        } else if (is_string_type(value_type->get_primitive_type())) {
            str += "\"" + value_type->to_string(nested_values_column, i) + "\"";
        } else {
            str += value_type->to_string(nested_values_column, i);
        }
    }
    str += "}";
    return str;
}

void DataTypeMap::to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const {
    std::string str = to_string(column, row_num);
    ostr.write(str.c_str(), str.size());
}

MutableColumnPtr DataTypeMap::create_column() const {
    return ColumnMap::create(key_type->create_column(), value_type->create_column(),
                             ColumnArray::ColumnOffsets::create());
}

Status DataTypeMap::check_column(const IColumn& column) const {
    const auto* column_map = DORIS_TRY(check_column_nested_type<ColumnMap>(column));
    RETURN_IF_ERROR(key_type->check_column(column_map->get_keys()));
    RETURN_IF_ERROR(value_type->check_column(column_map->get_values()));
    return Status::OK();
}

void DataTypeMap::to_pb_column_meta(PColumnMeta* col_meta) const {
    IDataType::to_pb_column_meta(col_meta);
    auto key_children = col_meta->add_children();
    auto value_children = col_meta->add_children();
    key_type->to_pb_column_meta(key_children);
    value_type->to_pb_column_meta(value_children);
}

bool DataTypeMap::equals(const IDataType& rhs) const {
    if (typeid(rhs) != typeid(*this)) {
        return false;
    }

    const DataTypeMap& rhs_map = static_cast<const DataTypeMap&>(rhs);

    if (!key_type->equals(*rhs_map.key_type)) {
        return false;
    }

    if (!value_type->equals(*rhs_map.value_type)) {
        return false;
    }

    return true;
}

// binary : const flag| row num | real saved num | offset | key_col | val_col
// offset : off1 | off2 ...
// key_col: key1 | key1 ...
// val_col: val1 | val2 ...
int64_t DataTypeMap::get_uncompressed_serialized_bytes(const IColumn& column,
                                                       int data_version) const {
    if (data_version >= USE_CONST_SERDE) {
        auto size = sizeof(bool) + sizeof(size_t) + sizeof(size_t);
        bool is_const_column = is_column_const(column);
        auto real_need_copy_num = is_const_column ? 1 : column.size();

        const IColumn* data_column = &column;
        if (is_const_column) {
            const auto& const_column = assert_cast<const ColumnConst&>(column);
            data_column = &(const_column.get_data_column());
        }
        const auto& map_column = assert_cast<const ColumnMap&>(*data_column);
        size = size + sizeof(ColumnArray::Offset64) * real_need_copy_num;
        size = size + get_key_type()->get_uncompressed_serialized_bytes(map_column.get_keys(),
                                                                        data_version);
        size = size + get_value_type()->get_uncompressed_serialized_bytes(map_column.get_values(),
                                                                          data_version);
        return size;
    } else {
        auto ptr = column.convert_to_full_column_if_const();
        const auto& data_column = assert_cast<const ColumnMap&>(*ptr.get());
        return sizeof(ColumnArray::Offset64) * (column.size() + 1) +
               get_key_type()->get_uncompressed_serialized_bytes(data_column.get_keys(),
                                                                 data_version) +
               get_value_type()->get_uncompressed_serialized_bytes(data_column.get_values(),
                                                                   data_version);
    }
}

// serialize to binary
char* DataTypeMap::serialize(const IColumn& column, char* buf, int be_exec_version) const {
    if (be_exec_version >= USE_CONST_SERDE) {
        const auto* data_column = &column;
        size_t real_need_copy_num = 0;
        buf = serialize_const_flag_and_row_num(&data_column, buf, &real_need_copy_num);

        const auto& map_column = assert_cast<const ColumnMap&>(*data_column);
        // offsets
        memcpy(buf, map_column.get_offsets().data(),
               real_need_copy_num * sizeof(ColumnArray::Offset64));
        buf += real_need_copy_num * sizeof(ColumnArray::Offset64);
        // key value
        buf = get_key_type()->serialize(map_column.get_keys(), buf, be_exec_version);
        return get_value_type()->serialize(map_column.get_values(), buf, be_exec_version);
    } else {
        auto ptr = column.convert_to_full_column_if_const();
        const auto& map_column = assert_cast<const ColumnMap&>(*ptr.get());

        // row num
        *reinterpret_cast<ColumnArray::Offset64*>(buf) = column.size();
        buf += sizeof(ColumnArray::Offset64);
        // offsets
        memcpy(buf, map_column.get_offsets().data(), column.size() * sizeof(ColumnArray::Offset64));
        buf += column.size() * sizeof(ColumnArray::Offset64);
        // key value
        buf = get_key_type()->serialize(map_column.get_keys(), buf, be_exec_version);
        return get_value_type()->serialize(map_column.get_values(), buf, be_exec_version);
    }
}
const char* DataTypeMap::deserialize(const char* buf, MutableColumnPtr* column,
                                     int be_exec_version) const {
    if (be_exec_version >= USE_CONST_SERDE) {
        auto* origin_column = column->get();
        size_t real_have_saved_num = 0;
        buf = deserialize_const_flag_and_row_num(buf, column, &real_have_saved_num);

        auto* map_column = assert_cast<ColumnMap*>(origin_column);
        auto& map_offsets = map_column->get_offsets();
        // offsets
        map_offsets.resize(real_have_saved_num);
        memcpy(map_offsets.data(), buf, sizeof(ColumnArray::Offset64) * real_have_saved_num);
        buf += sizeof(ColumnArray::Offset64) * real_have_saved_num;
        // key value
        auto nested_keys_column = map_column->get_keys_ptr()->assume_mutable();
        auto nested_values_column = map_column->get_values_ptr()->assume_mutable();
        buf = get_key_type()->deserialize(buf, &nested_keys_column, be_exec_version);
        buf = get_value_type()->deserialize(buf, &nested_values_column, be_exec_version);
        return buf;
    } else {
        auto* map_column = assert_cast<ColumnMap*>(column->get());
        auto& map_offsets = map_column->get_offsets();
        // row num
        ColumnArray::Offset64 row_num = *reinterpret_cast<const ColumnArray::Offset64*>(buf);
        buf += sizeof(ColumnArray::Offset64);
        // offsets
        map_offsets.resize(row_num);
        memcpy(map_offsets.data(), buf, sizeof(ColumnArray::Offset64) * row_num);
        buf += sizeof(ColumnArray::Offset64) * row_num;
        // key value
        auto nested_keys_column = map_column->get_keys_ptr()->assume_mutable();
        auto nested_values_column = map_column->get_values_ptr()->assume_mutable();
        buf = get_key_type()->deserialize(buf, &nested_keys_column, be_exec_version);
        return get_value_type()->deserialize(buf, &nested_values_column, be_exec_version);
    }
}
} // namespace doris::vectorized