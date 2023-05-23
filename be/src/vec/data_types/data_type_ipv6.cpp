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
    IPv6 value = assert_cast<const ColumnIPv6&>(*ptr).get_element(row_num);
    return convert_ipv6_to_string(value);
}

void DataTypeIPv6::to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const {
    std::string value = to_string(column, row_num);
    ostr.write(value.data(), value.size());
}

Status DataTypeIPv6::from_string(ReadBuffer& rb, IColumn* column) const {
    auto* column_data = static_cast<ColumnIPv6*>(column);
    IPv6 value;
    if (!convert_string_to_ipv6(value, rb.to_string())) {
        throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                               "Invalid value: {} for type IPv6", rb.to_string());
    }
    column_data->insert_value(value);
    return Status::OK();
}

std::string DataTypeIPv6::convert_ipv6_to_string(IPv6 ipv6) {
    uint16_t fields[8] = {
            static_cast<uint16_t>((ipv6.high >> 48) & 0xFFFF),
            static_cast<uint16_t>((ipv6.high >> 32) & 0xFFFF),
            static_cast<uint16_t>((ipv6.high >> 16) & 0xFFFF),
            static_cast<uint16_t>(ipv6.high & 0xFFFF),
            static_cast<uint16_t>((ipv6.low >> 48) & 0xFFFF),
            static_cast<uint16_t>((ipv6.low >> 32) & 0xFFFF),
            static_cast<uint16_t>((ipv6.low >> 16) & 0xFFFF),
            static_cast<uint16_t>(ipv6.low & 0xFFFF)
    };

    uint8_t zero_start = 0, zero_end = 0;

    while (zero_start < 8 && zero_end < 8) {
        if (fields[zero_start] != 0) {
            zero_start++;
            zero_end = zero_start;
            continue;
        }

        while (zero_end < 7 && fields[zero_end + 1] == 0) {
            zero_end++;
        }

        if (zero_end > zero_start) {
            break;
        }

        zero_start++;
        zero_end = zero_start;
    }

    std::stringstream ss;

    if (zero_start == zero_end) {
        for (uint8_t i = 0; i < 7; ++i) {
            ss << std::hex << fields[i] << ":";
        }
        ss << std::hex << fields[7];
    } else {
        for (uint8_t i = 0; i < zero_start; ++i) {
            ss << std::hex << fields[i] << ":";
        }

        if (zero_end == 7) {
            ss << ":";
        } else {
            for (uint8_t j = zero_end + 1; j < 8; ++j) {
                ss << std::hex << ":" << fields[j];
            }
        }
    }

    return ss.str();
}

bool DataTypeIPv6::convert_string_to_ipv6(IPv6& x, std::string ipv6) {
    std::istringstream iss(ipv6);
    std::string field;
    uint16_t fields[8] = {0};
    uint8_t zero_index = 0;
    uint8_t num_field = 0;
    uint8_t right_field_num = 0;

    while (num_field < 8) {
        if (!getline(iss, field, ':')) {
            break;
        }

        if (field.empty()) {
            zero_index = num_field;
            fields[num_field++] = 0;
        } else {
            try {
                fields[num_field++] = std::stoi(field, nullptr, 16);
            } catch (const std::exception& e) {
                return false;
            }
        }
    }

    if (zero_index != 0) {
        right_field_num = num_field - zero_index - 1;

        for (uint8_t i = 7; i > 7 - right_field_num; --i) {
            fields[i] = fields[zero_index + right_field_num + i - 7];
            fields[zero_index + right_field_num + i - 7] = 0;
        }
    }

    x.high = (static_cast<uint64_t>(fields[0]) << 48) | (static_cast<uint64_t>(fields[1]) << 32) |
             (static_cast<uint64_t>(fields[2]) << 16) | static_cast<uint64_t>(fields[3]);
    x.low = (static_cast<uint64_t>(fields[4]) << 48) | (static_cast<uint64_t>(fields[5]) << 32) |
            (static_cast<uint64_t>(fields[6]) << 16) | static_cast<uint64_t>(fields[7]);

    return true;
}

MutableColumnPtr DataTypeIPv6::create_column() const {
    return ColumnIPv6::create();
}

} // namespace doris::vectorized
