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
#include "vec/runtime/ipv6_value.h"

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
    return IPv6Value::to_string(ipv6);
}

bool DataTypeIPv6::convert_string_to_ipv6(IPv6& x, std::string ipv6) {
    return IPv6Value::from_string(x, ipv6);
}

MutableColumnPtr DataTypeIPv6::create_column() const {
    return ColumnIPv6::create();
}

} // namespace doris::vectorized
