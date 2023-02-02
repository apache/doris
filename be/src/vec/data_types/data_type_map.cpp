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

#include "gen_cpp/data.pb.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_map.h"
#include "vec/common/assert_cast.h"
#include "vec/data_types/data_type_factory.hpp"

namespace doris::vectorized {

DataTypeMap::DataTypeMap(const DataTypePtr& keys_, const DataTypePtr& values_) {
    key_type = keys_;
    value_type = values_;

    keys = std::make_shared<DataTypeArray>(key_type);
    values = std::make_shared<DataTypeArray>(value_type);
}

std::string DataTypeMap::to_string(const IColumn& column, size_t row_num) const {
    const ColumnMap& map_column = assert_cast<const ColumnMap&>(column);
    const ColumnArray::Offsets64& offsets = map_column.get_offsets();

    size_t offset = offsets[row_num - 1];
    size_t next_offset = offsets[row_num];

    auto& keys_arr = assert_cast<const ColumnArray&>(map_column.get_keys());
    auto& values_arr = assert_cast<const ColumnArray&>(map_column.get_values());

    const IColumn& nested_keys_column = keys_arr.get_data();
    const IColumn& nested_values_column = values_arr.get_data();

    std::stringstream ss;
    ss << "{";
    for (size_t i = offset; i < next_offset; ++i) {
        if (i != offset) {
            ss << ", ";
        }
        if (nested_keys_column.is_null_at(i)) {
            ss << "NULL";
        } else if (WhichDataType(remove_nullable(key_type)).is_string_or_fixed_string()) {
            ss << "'" << key_type->to_string(nested_keys_column, i) << "'";
        } else {
            ss << key_type->to_string(nested_keys_column, i);
        }
        ss << ":";
        if (nested_values_column.is_null_at(i)) {
            ss << "NULL";
        } else if (WhichDataType(remove_nullable(value_type)).is_string_or_fixed_string()) {
            ss << "'" << value_type->to_string(nested_values_column, i) << "'";
        } else {
            ss << value_type->to_string(nested_values_column, i);
        }
    }
    ss << "}";
    return ss.str();
}

void DataTypeMap::to_string(const class doris::vectorized::IColumn& column, size_t row_num,
                            class doris::vectorized::BufferWritable& ostr) const {
    std::string ss = to_string(column, row_num);
    ostr.write(ss.c_str(), strlen(ss.c_str()));
}

Status DataTypeMap::from_string(ReadBuffer& rb, IColumn* column) const {
    DCHECK(!rb.eof());
    auto* map_column = assert_cast<ColumnMap*>(column);

    if (*rb.position() != '{') {
        return Status::InvalidArgument("map does not start with '{' character, found '{}'",
                                       *rb.position());
    }
    if (*(rb.end() - 1) != '}') {
        return Status::InvalidArgument("map does not end with '}' character, found '{}'",
                                       *(rb.end() - 1));
    }

    std::stringstream keyCharset;
    std::stringstream valCharset;

    if (rb.count() == 2) {
        // empty map {} , need to make empty array to add offset
        keyCharset << "[]";
        valCharset << "[]";
    } else {
        // {"aaa": 1, "bbb": 20}, need to handle key and value to make key column arr and value arr
        // skip "{"
        ++rb.position();
        keyCharset << "[";
        valCharset << "[";
        while (!rb.eof()) {
            size_t kv_len = 0;
            auto start = rb.position();
            while (!rb.eof() && *start != ',' && *start != '}') {
                kv_len++;
                start++;
            }
            if (kv_len >= rb.count()) {
                return Status::InvalidArgument("Invalid Length");
            }

            size_t k_len = 0;
            auto k_rb = rb.position();
            while (kv_len > 0 && *k_rb != ':') {
                k_len++;
                k_rb++;
            }
            ReadBuffer key_rb(rb.position(), k_len);
            ReadBuffer val_rb(k_rb + 1, kv_len - k_len - 1);

            // handle key
            keyCharset << key_rb.to_string();
            keyCharset << ",";

            // handle value
            valCharset << val_rb.to_string();
            valCharset << ",";

            rb.position() += kv_len + 1;
        }
        keyCharset << ']';
        valCharset << ']';
    }

    ReadBuffer kb(keyCharset.str().data(), keyCharset.str().length());
    ReadBuffer vb(valCharset.str().data(), valCharset.str().length());
    keys->from_string(kb, &map_column->get_keys());
    values->from_string(vb, &map_column->get_values());
    return Status::OK();
}

MutableColumnPtr DataTypeMap::create_column() const {
    return ColumnMap::create(keys->create_column(), values->create_column());
}

void DataTypeMap::to_pb_column_meta(PColumnMeta* col_meta) const {
    IDataType::to_pb_column_meta(col_meta);
    auto key_children = col_meta->add_children();
    auto value_children = col_meta->add_children();
    keys->to_pb_column_meta(key_children);
    values->to_pb_column_meta(value_children);
}

bool DataTypeMap::equals(const IDataType& rhs) const {
    if (typeid(rhs) != typeid(*this)) {
        return false;
    }

    const DataTypeMap& rhs_map = static_cast<const DataTypeMap&>(rhs);

    if (!keys->equals(*rhs_map.keys)) {
        return false;
    }

    if (!values->equals(*rhs_map.values)) {
        return false;
    }

    return true;
}

int64_t DataTypeMap::get_uncompressed_serialized_bytes(const IColumn& column,
                                                       int data_version) const {
    auto ptr = column.convert_to_full_column_if_const();
    const auto& data_column = assert_cast<const ColumnMap&>(*ptr.get());
    return get_keys()->get_uncompressed_serialized_bytes(data_column.get_keys(), data_version) +
           get_values()->get_uncompressed_serialized_bytes(data_column.get_values(), data_version);
}

// serialize to binary
char* DataTypeMap::serialize(const IColumn& column, char* buf, int data_version) const {
    auto ptr = column.convert_to_full_column_if_const();
    const auto& map_column = assert_cast<const ColumnMap&>(*ptr.get());

    buf = get_keys()->serialize(map_column.get_keys(), buf, data_version);
    return get_values()->serialize(map_column.get_values(), buf, data_version);
}

const char* DataTypeMap::deserialize(const char* buf, IColumn* column, int data_version) const {
    const auto* map_column = assert_cast<const ColumnMap*>(column);
    buf = get_keys()->deserialize(buf, map_column->get_keys_ptr()->assume_mutable(), data_version);
    return get_values()->deserialize(buf, map_column->get_values_ptr()->assume_mutable(),
                                     data_version);
}

} // namespace doris::vectorized
