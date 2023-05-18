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

#include "vec/data_types/data_type_ipv6.h"


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
bool DataTypeIPv6::equals(const IDataType& rhs) const {
    return typeid(rhs) == typeid(*this);
}

std::string DataTypeIPv6::to_string(const IColumn& column, size_t row_num) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;
    uint128_t value = assert_cast<const ColumnUInt128&>(*ptr).get_element(row_num);
    return convert_ipv6_to_string(value);
}

void DataTypeIPv6::to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const {
    std::string value = to_string(column, row_num);
    ostr.write(value.data(), value.size());
}

Status DataTypeIPv6::from_string(ReadBuffer& rb, IColumn* column) const {
    auto* column_data = static_cast<ColumnUInt128*>(column);
    uint128_t value;
    if (!convert_string_to_ipv6(value, rb.to_string())) {
        throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                               "Invalid value: {} for type IPv6", rb.to_string());
    }
    column_data->insert_value(value);
    return Status::OK();
}

std::string DataTypeIPv6::convert_ipv6_to_string(uint128_t ipv6) {
    std::stringstream result;

    for (int i = 0; i < 8; i++) {
        uint16_t part = static_cast<uint16_t>((ipv6 >> (112 - i * 16)) & 0xFFFF);
        result << std::to_string(part);
        if (i != 7) {
            result << ":";
        }
    }

    return result.str();
}

bool DataTypeIPv6::convert_string_to_ipv6(uint128_t& x, std::string ipv6) {
    std::istringstream iss(ipv6);
    std::string token;
    uint128_t result = 0;
    int count = 0;

    while (std::getline(iss, token, ':')) {
        if (token.empty()) {
            count += 8 - count;
            break;
        }

        if (count > 8) {
            return false;
        }

        uint16_t value;
        std::istringstream ss(token);
        if (!(ss >> std::hex >> value)) {
            return false;
        }

        result = (result << 16) | value;
        count++;
    }

    if (count < 8) {
        return false;
    }

    x = result;
    return true;
}

MutableColumnPtr DataTypeIPv6::create_column() const {
    return DataTypeNumberBase<UInt128>::create_column();
}

} // namespace doris::vectorized
