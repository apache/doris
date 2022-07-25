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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/DataTypeArray.h
// and modified by Doris

#include "vec/data_types/data_type_array.h"

#include "gen_cpp/data.pb.h"
#include "vec/common/string_utils/string_utils.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

namespace ErrorCodes {
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

DataTypeArray::DataTypeArray(const DataTypePtr& nested_) : nested {nested_} {}

MutableColumnPtr DataTypeArray::create_column() const {
    return ColumnArray::create(nested->create_column(), ColumnArray::ColumnOffsets::create());
}

Field DataTypeArray::get_default() const {
    return Array();
}

bool DataTypeArray::equals(const IDataType& rhs) const {
    return typeid(rhs) == typeid(*this) &&
           nested->equals(*static_cast<const DataTypeArray&>(rhs).nested);
}

size_t DataTypeArray::get_number_of_dimensions() const {
    const DataTypeArray* nested_array = typeid_cast<const DataTypeArray*>(nested.get());
    if (!nested_array) return 1;
    return 1 +
           nested_array
                   ->get_number_of_dimensions(); /// Every modern C++ compiler optimizes tail recursion.
}

int64_t DataTypeArray::get_uncompressed_serialized_bytes(const IColumn& column) const {
    auto ptr = column.convert_to_full_column_if_const();
    const auto& data_column = assert_cast<const ColumnArray&>(*ptr.get());
    return sizeof(IColumn::Offset) * (column.size() + 1) +
           get_nested_type()->get_uncompressed_serialized_bytes(data_column.get_data());
}

char* DataTypeArray::serialize(const IColumn& column, char* buf) const {
    auto ptr = column.convert_to_full_column_if_const();
    const auto& data_column = assert_cast<const ColumnArray&>(*ptr.get());

    // row num
    *reinterpret_cast<IColumn::Offset*>(buf) = column.size();
    buf += sizeof(IColumn::Offset);
    // offsets
    memcpy(buf, data_column.get_offsets().data(), column.size() * sizeof(IColumn::Offset));
    buf += column.size() * sizeof(IColumn::Offset);
    // children
    return get_nested_type()->serialize(data_column.get_data(), buf);
}

const char* DataTypeArray::deserialize(const char* buf, IColumn* column) const {
    auto* data_column = assert_cast<ColumnArray*>(column);
    auto& offsets = data_column->get_offsets();

    // row num
    IColumn::Offset row_num = *reinterpret_cast<const IColumn::Offset*>(buf);
    buf += sizeof(IColumn::Offset);
    // offsets
    offsets.resize(row_num);
    memcpy(offsets.data(), buf, sizeof(IColumn::Offset) * row_num);
    buf += sizeof(IColumn::Offset) * row_num;
    // children
    return get_nested_type()->deserialize(buf, data_column->get_data_ptr()->assume_mutable());
}

void DataTypeArray::to_pb_column_meta(PColumnMeta* col_meta) const {
    IDataType::to_pb_column_meta(col_meta);
    auto children = col_meta->add_children();
    get_nested_type()->to_pb_column_meta(children);
}

void DataTypeArray::to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const {
    auto ptr = column.convert_to_full_column_if_const();
    auto& data_column = assert_cast<const ColumnArray&>(*ptr.get());
    auto& offsets = data_column.get_offsets();

    size_t offset = offsets[row_num - 1];
    size_t next_offset = offsets[row_num];

    const IColumn& nested_column = data_column.get_data();
    ostr.write("[", 1);
    for (size_t i = offset; i < next_offset; ++i) {
        if (i != offset) {
            ostr.write(",", 1);
        }
        nested->to_string(nested_column, i, ostr);
    }
    ostr.write("]", 1);
}

std::string DataTypeArray::to_string(const IColumn& column, size_t row_num) const {
    auto ptr = column.convert_to_full_column_if_const();
    auto& data_column = assert_cast<const ColumnArray&>(*ptr.get());
    auto& offsets = data_column.get_offsets();

    size_t offset = offsets[row_num - 1];
    size_t next_offset = offsets[row_num];
    const IColumn& nested_column = data_column.get_data();
    std::stringstream ss;
    ss << "[";
    for (size_t i = offset; i < next_offset; ++i) {
        if (i != offset) {
            ss << ",";
        }
        ss << nested->to_string(nested_column, i);
    }
    ss << "]";
    return ss.str();
}

Status DataTypeArray::from_string(ReadBuffer& rb, IColumn* column) const {
    // only support one level now
    auto* array_column = assert_cast<ColumnArray*>(column);
    auto& offsets = array_column->get_offsets();

    IColumn& nested_column = array_column->get_data();
    if (*rb.position() != '[') {
        return Status::InvalidArgument("Array does not start with '[' character, found '{}'",
                                       *rb.position());
    }
    ++rb.position();
    bool first = true;
    size_t size = 0;
    while (!rb.eof() && *rb.position() != ']') {
        if (!first) {
            if (*rb.position() == ',') {
                ++rb.position();
            } else {
                return Status::InvalidArgument(
                        "Cannot read array from text, expected comma or end of array, found '{}'",
                        *rb.position());
            }
        }
        first = false;
        if (*rb.position() == ']') {
            break;
        }
        size_t nested_str_len = 1;
        char* temp_char = rb.position() + nested_str_len;
        while (*(temp_char) != ']' && *(temp_char) != ',' && temp_char != rb.end()) {
            ++nested_str_len;
            temp_char = rb.position() + nested_str_len;
        }

        // dispose the case of ["123"] or ['123']
        ReadBuffer read_buffer(rb.position(), nested_str_len);
        auto begin_char = *rb.position();
        auto end_char = *(rb.position() + nested_str_len - 1);
        if (begin_char == end_char && (begin_char == '"' || begin_char == '\'')) {
            read_buffer = ReadBuffer(rb.position() + 1, nested_str_len - 2);
        }

        auto st = nested->from_string(read_buffer, &nested_column);
        if (!st.ok()) {
            // we should do revert if error
            array_column->pop_back(size);
            return st;
        }
        rb.position() += nested_str_len;
        DCHECK_LE(rb.position(), rb.end());
        ++size;
    }
    offsets.push_back(offsets.back() + size);
    return Status::OK();
}

} // namespace doris::vectorized
