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

#include <ctype.h>
#include <gen_cpp/data.pb.h>
#include <glog/logging.h>
#include <string.h>

#include <typeinfo>
#include <utility>

#include "runtime/decimalv2_value.h"
#include "util/types.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_buffer.hpp"
#include "vec/common/string_ref.h"
#include "vec/common/typeid_cast.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/io/reader_buffer.h"

namespace doris::vectorized {

namespace ErrorCodes {
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

DataTypeArray::DataTypeArray(const DataTypePtr& nested_) : nested {nested_} {}

MutableColumnPtr DataTypeArray::create_column() const {
    return ColumnArray::create(nested->create_column(), ColumnArray::ColumnOffsets::create());
}

Field DataTypeArray::get_default() const {
    Array a;
    a.push_back(nested->get_default());
    return a;
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

int64_t DataTypeArray::get_uncompressed_serialized_bytes(const IColumn& column,
                                                         int be_exec_version) const {
    auto ptr = column.convert_to_full_column_if_const();
    const auto& data_column = assert_cast<const ColumnArray&>(*ptr.get());
    return sizeof(ColumnArray::Offset64) * (column.size() + 1) +
           get_nested_type()->get_uncompressed_serialized_bytes(data_column.get_data(),
                                                                be_exec_version);
}

char* DataTypeArray::serialize(const IColumn& column, char* buf, int be_exec_version) const {
    auto ptr = column.convert_to_full_column_if_const();
    const auto& data_column = assert_cast<const ColumnArray&>(*ptr.get());

    // row num
    *reinterpret_cast<ColumnArray::Offset64*>(buf) = column.size();
    buf += sizeof(ColumnArray::Offset64);
    // offsets
    memcpy(buf, data_column.get_offsets().data(), column.size() * sizeof(ColumnArray::Offset64));
    buf += column.size() * sizeof(ColumnArray::Offset64);
    // children
    return get_nested_type()->serialize(data_column.get_data(), buf, be_exec_version);
}

const char* DataTypeArray::deserialize(const char* buf, IColumn* column,
                                       int be_exec_version) const {
    auto* data_column = assert_cast<ColumnArray*>(column);
    auto& offsets = data_column->get_offsets();

    // row num
    ColumnArray::Offset64 row_num = *reinterpret_cast<const ColumnArray::Offset64*>(buf);
    buf += sizeof(ColumnArray::Offset64);
    // offsets
    offsets.resize(row_num);
    memcpy(offsets.data(), buf, sizeof(ColumnArray::Offset64) * row_num);
    buf += sizeof(ColumnArray::Offset64) * row_num;
    // children
    return get_nested_type()->deserialize(buf, data_column->get_data_ptr()->assume_mutable(),
                                          be_exec_version);
}

void DataTypeArray::to_pb_column_meta(PColumnMeta* col_meta) const {
    IDataType::to_pb_column_meta(col_meta);
    auto children = col_meta->add_children();
    get_nested_type()->to_pb_column_meta(children);
}

void DataTypeArray::to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    auto& data_column = assert_cast<const ColumnArray&>(*ptr);
    auto& offsets = data_column.get_offsets();

    size_t offset = offsets[row_num - 1];
    size_t next_offset = offsets[row_num];

    const IColumn& nested_column = data_column.get_data();
    ostr.write("[", 1);
    for (size_t i = offset; i < next_offset; ++i) {
        if (i != offset) {
            ostr.write(", ", 2);
        }
        WhichDataType which(remove_nullable(nested));
        if (which.is_string_or_fixed_string()) {
            ostr.write("'", 1);
            nested->to_string(nested_column, i, ostr);
            ostr.write("'", 1);
        } else {
            nested->to_string(nested_column, i, ostr);
        }
    }
    ostr.write("]", 1);
}

std::string DataTypeArray::to_string(const IColumn& column, size_t row_num) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    auto& data_column = assert_cast<const ColumnArray&>(*ptr);
    auto& offsets = data_column.get_offsets();

    size_t offset = offsets[row_num - 1];
    size_t next_offset = offsets[row_num];
    const IColumn& nested_column = data_column.get_data();
    std::string str;
    str += "[";
    for (size_t i = offset; i < next_offset; ++i) {
        if (i != offset) {
            str += ", ";
        }
        WhichDataType which(remove_nullable(nested));
        if (which.is_string_or_fixed_string()) {
            str += "'";
            str += nested->to_string(nested_column, i);
            str += "'";
        } else {
            str += nested->to_string(nested_column, i);
        }
    }
    str += "]";
    return str;
}

bool next_element_from_string(ReadBuffer& rb, StringRef& output, bool& has_quota) {
    StringRef element(rb.position(), 0);
    has_quota = false;
    if (rb.eof()) {
        return false;
    }

    // ltrim
    while (!rb.eof() && isspace(*rb.position())) {
        ++rb.position();
        element.data = rb.position();
    }

    // parse string
    if (*rb.position() == '"' || *rb.position() == '\'') {
        const char str_sep = *rb.position();
        size_t str_len = 1;
        // search until next '"' or '\''
        while (str_len < rb.count() && *(rb.position() + str_len) != str_sep) {
            ++str_len;
        }
        // invalid string
        if (str_len >= rb.count()) {
            rb.position() = rb.end();
            return false;
        }
        has_quota = true;
        rb.position() += str_len + 1;
        element.size += str_len + 1;
    }

    // parse array element until array separator ',' or end ']'
    while (!rb.eof() && (*rb.position() != ',') && (rb.count() != 1 || *rb.position() != ']')) {
        // invalid elements such as ["123" 456,"789" 777]
        // correct elements such as ["123"    ,"789"    ]
        if (has_quota && !isspace(*rb.position())) {
            return false;
        }
        ++rb.position();
        ++element.size;
    }
    // invalid array element
    if (rb.eof()) {
        return false;
    }
    // adjust read buffer position to first char of next array element
    ++rb.position();

    // rtrim
    while (element.size > 0 && isspace(element.data[element.size - 1])) {
        --element.size;
    }

    // trim '"' and '\'' for string
    if (element.size >= 2 && (element.data[0] == '"' || element.data[0] == '\'') &&
        element.data[0] == element.data[element.size - 1]) {
        ++element.data;
        element.size -= 2;
    }
    output = element;
    return true;
}

Status DataTypeArray::from_string(ReadBuffer& rb, IColumn* column) const {
    DCHECK(!rb.eof());
    // only support one level now
    auto* array_column = assert_cast<ColumnArray*>(column);
    auto& offsets = array_column->get_offsets();

    IColumn& nested_column = array_column->get_data();
    DCHECK(nested_column.is_nullable());
    if (*rb.position() != '[') {
        return Status::InvalidArgument("Array does not start with '[' character, found '{}'",
                                       *rb.position());
    }
    if (*(rb.end() - 1) != ']') {
        return Status::InvalidArgument("Array does not end with ']' character, found '{}'",
                                       *(rb.end() - 1));
    }
    // empty array []
    if (rb.count() == 2) {
        offsets.push_back(offsets.back());
        return Status::OK();
    }
    ++rb.position();

    size_t element_num = 0;
    // parse array element until end of array
    while (!rb.eof()) {
        StringRef element(rb.position(), rb.count());
        bool has_quota = false;
        if (!next_element_from_string(rb, element, has_quota)) {
            // we should do array element column revert if error
            nested_column.pop_back(element_num);
            return Status::InvalidArgument("Cannot read array element from text '{}'",
                                           element.to_string());
        }

        // handle empty element
        if (element.size == 0) {
            auto& nested_null_col = reinterpret_cast<ColumnNullable&>(nested_column);
            nested_null_col.get_nested_column().insert_default();
            nested_null_col.get_null_map_data().push_back(0);
            ++element_num;
            continue;
        }

        // handle null element, need to distinguish null and "null"
        if (!has_quota && element.size == 4 && strncmp(element.data, "null", 4) == 0) {
            // insert null
            auto& nested_null_col = reinterpret_cast<ColumnNullable&>(nested_column);
            nested_null_col.get_nested_column().insert_default();
            nested_null_col.get_null_map_data().push_back(1);
            ++element_num;
            continue;
        }

        // handle normal element
        ReadBuffer read_buffer(const_cast<char*>(element.data), element.size);
        auto st = nested->from_string(read_buffer, &nested_column);
        if (!st.ok()) {
            // we should do array element column revert if error
            nested_column.pop_back(element_num);
            return st;
        }
        ++element_num;
    }
    offsets.push_back(offsets.back() + element_num);
    return Status::OK();
}

} // namespace doris::vectorized
