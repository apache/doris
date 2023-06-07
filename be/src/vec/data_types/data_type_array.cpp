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

void get_decimal_value(const IColumn& nested_column, DecimalV2Value& decimal_value, size_t pos) {
    const IColumn* nested_col = &nested_column;
    if (nested_column.is_nullable()) {
        nested_col =
                reinterpret_cast<const ColumnNullable*>(&nested_column)->get_nested_column_ptr();
    }
    decimal_value = (DecimalV2Value)(reinterpret_cast<const PackedInt128*>(
                                             nested_col->get_data_at(pos).data)
                                             ->value);
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

Status DataTypeArray::from_json(simdjson::ondemand::value& json_value, IColumn* column) const {
    CHECK(json_value.type() == simdjson::ondemand::json_type::array);
    simdjson::ondemand::array outer_array = json_value.get_array();
    auto* array_column = assert_cast<ColumnArray*>(column);
    auto& offsets = array_column->get_offsets();
    IColumn& nested_column = array_column->get_data();
    DCHECK(nested_column.is_nullable());
    auto& nested_null_col = reinterpret_cast<ColumnNullable&>(nested_column);

    size_t element_num = 0;
    for (auto it = outer_array.begin(); it != outer_array.end(); ++it) {
        Status st;
        try {
            if (is_complex_type(remove_nullable(nested))) {
                simdjson::ondemand::value val;
                (*it).get(val);
                st = nested->from_json(val, &nested_null_col);
            } else {
                std::string_view sv = (*it).raw_json_token().value();
                ReadBuffer nested_rb(const_cast<char*>(sv.data()), sv.size());
                st = nested->from_string(nested_rb, &nested_column);
            }
            if (st != Status::OK()) {
                // we should do array element column revert if error
                nested_column.pop_back(element_num);
                return st;
            }
        } catch (simdjson::simdjson_error& e) {
            nested_column.pop_back(element_num);
            fmt::memory_buffer error_msg;
            fmt::format_to(error_msg, "Parse json data failed. code: {}, error info: {}", e.error(),
                           e.what());
            return Status::InternalError(error_msg.data());
        }
        ++element_num;
    }
    offsets.push_back(offsets.back() + element_num);
    return Status::OK();
}

Status DataTypeArray::from_string(ReadBuffer& rb, IColumn* column) const {
    // check staff
    DCHECK(!rb.eof());
    if (*rb.position() != '[') {
        return Status::InvalidArgument("Array does not start with '[' character, found '{}'",
                                       *rb.position());
    }
    if (*(rb.end() - 1) != ']') {
        return Status::InvalidArgument("Array does not end with ']' character, found '{}'",
                                       *(rb.end() - 1));
    }
    // json parser
    std::unique_ptr<simdjson::ondemand::parser> _ondemand_json_parser =
            std::make_unique<simdjson::ondemand::parser>();
    size_t _padded_size = rb.count() + simdjson::SIMDJSON_PADDING;
    std::string _simdjson_ondemand_padding_buffer;
    _simdjson_ondemand_padding_buffer.resize(_padded_size);
    memcpy(&_simdjson_ondemand_padding_buffer.front(), rb.position(), rb.count());
    simdjson::ondemand::document array_doc = _ondemand_json_parser->iterate(
            std::string_view(_simdjson_ondemand_padding_buffer.data(), rb.count()), _padded_size);
    auto value = array_doc.get_value();
    return from_json(value.value(), column);
}
} // namespace doris::vectorized
