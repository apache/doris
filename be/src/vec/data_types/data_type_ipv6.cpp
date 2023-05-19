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
    UInt128 value = assert_cast<const ColumnUInt128&>(*ptr).get_element(row_num);
    return convert_ipv6_to_string(value);
}

void DataTypeIPv6::to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const {
    std::string value = to_string(column, row_num);
    ostr.write(value.data(), value.size());
}

Status DataTypeIPv6::from_string(ReadBuffer& rb, IColumn* column) const {
    auto* column_data = static_cast<ColumnUInt128*>(column);
    UInt128 value;
    if (!convert_string_to_ipv6(value, rb.to_string())) {
        throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                               "Invalid value: {} for type IPv6", rb.to_string());
    }
    column_data->insert_value(value);
    return Status::OK();
}

std::string DataTypeIPv6::convert_ipv6_to_string(UInt128 ipv6) {
    std::stringstream ss;
    ss << std::hex << std::setfill('0');

    for (int i = 7; i >= 0; i--) {
        UInt16 field = (i < 4) ? ((ipv6.low >> (16 * i)) & 0xFFFF) : ((ipv6.high >> (16 * (i - 4))) & 0xFFFF);
        ss << std::setw(4) << field;
        if (i != 0) {
            ss << ":";
        }
    }

    return ss.str();
}

bool DataTypeIPv6::convert_string_to_ipv6(UInt128& x, std::string ipv6) {
    std::stringstream ss(ipv6);

    UInt16 fields[8];
    char delimiter;
    for (int i = 0; i < 8; i++) {
        if (!(ss >> std::hex >> fields[i])) {
            return false;
        }
        if (i < 7 && !(ss >> delimiter) && delimiter != ':') {
            return false;
        }
    }

    x.low = 0;
    x.high = 0;
    for (int i = 0; i < 4; i++) {
        x.low |= static_cast<UInt64>(fields[i]) << (16 * (3 - i));
    }
    for (int i = 4; i < 8; i++) {
        x.high |= static_cast<UInt64>(fields[i]) << (16 * (7 - i));
    }

    return true;
}

MutableColumnPtr DataTypeIPv6::create_column() const {
    return DataTypeNumberBase<UInt128>::create_column();
}

} // namespace doris::vectorized
