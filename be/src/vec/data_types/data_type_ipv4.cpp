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

#include "vec/data_types/data_type_ipv4.h"

#include "util/binary_cast.hpp"
#include "util/string_parser.hpp"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_buffer.hpp"
#include "vec/data_types/data_type.h"
#include "vec/io/io_helper.h"
#include "vec/io/reader_buffer.h"

namespace doris::vectorized {
bool DataTypeIPv4::equals(const IDataType& rhs) const {
    return typeid(rhs) == typeid(*this);
}

std::string DataTypeIPv4::to_string(const IColumn& column, size_t row_num) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;
    IPv4 value = assert_cast<const ColumnIPv4&>(*ptr).get_element(row_num);
    return convert_ipv4_to_string(value);
}

void DataTypeIPv4::to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const {
    std::string value = to_string(column, row_num);
    ostr.write(value.data(), value.size());
}

Status DataTypeIPv4::from_string(ReadBuffer& rb, IColumn* column) const {
    auto* column_data = static_cast<ColumnIPv4*>(column);
    StringParser::ParseResult result;
    IPv4 val = StringParser::string_to_unsigned_int<IPv4>(rb.position(), rb.count(), &result);
    column_data->insert_value(val);
    return Status::OK();
}

std::string DataTypeIPv4::convert_ipv4_to_string(IPv4 ipv4) {
    std::stringstream ss;
    ss << ((ipv4 >> 24) & 0xFF) << '.' << ((ipv4 >> 16) & 0xFF) << '.' << ((ipv4 >> 8) & 0xFF)
       << '.' << (ipv4 & 0xFF);
    return ss.str();
}

bool DataTypeIPv4::convert_string_to_ipv4(IPv4& x, std::string ipv4) {
    const static int IPV4_PARTS_NUM = 4;
    IPv4 parts[IPV4_PARTS_NUM];
    int part_index = 0;
    std::stringstream ss(ipv4);
    std::string part;
    StringParser::ParseResult result;

    while (std::getline(ss, part, '.')) {
        IPv4 val = StringParser::string_to_unsigned_int<IPv4>(part.data(), part.size(), &result);
        if (UNLIKELY(result != StringParser::PARSE_SUCCESS) || val > 255) {
            return false;
        }
        parts[part_index++] = val;
    }

    if (part_index != 4) {
        return false;
    }

    x = (parts[0] << 24) | (parts[1] << 16) | (parts[2] << 8) | parts[3];
    return true;
}

MutableColumnPtr DataTypeIPv4::create_column() const {
    return ColumnIPv4::create();
}

} // namespace doris::vectorized
