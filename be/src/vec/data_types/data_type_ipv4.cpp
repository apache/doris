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

size_t DataTypeIPv4::number_length() const {
    //255.255.255.255
    return 16;
}
void DataTypeIPv4::push_number(ColumnString::Chars& chars, const IPv4& num) const {
    auto value = IPv4Value(num);
    auto ipv4_str = value.to_string();
    chars.insert(ipv4_str.begin(), ipv4_str.end());
}

std::string DataTypeIPv4::to_string(const IColumn& column, size_t row_num) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;
    IPv4 ipv4_val = assert_cast<const ColumnIPv4&>(*ptr).get_element(row_num);
    auto value = IPv4Value(ipv4_val);
    return value.to_string();
}

std::string DataTypeIPv4::to_string(const IPv4& ipv4_val) const {
    auto value = IPv4Value(ipv4_val);
    return value.to_string();
}

void DataTypeIPv4::to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const {
    std::string value = to_string(column, row_num);
    ostr.write(value.data(), value.size());
}

Status DataTypeIPv4::from_string(ReadBuffer& rb, IColumn* column) const {
    auto* column_data = static_cast<ColumnIPv4*>(column);
    IPv4 val = 0;
    if (!read_ipv4_text_impl<IPv4>(val, rb)) {
        return Status::InvalidArgument("parse ipv4 fail, string: '{}'",
                                       std::string(rb.position(), rb.count()).c_str());
    }
    column_data->insert_value(val);
    return Status::OK();
}

MutableColumnPtr DataTypeIPv4::create_column() const {
    return ColumnIPv4::create();
}

} // namespace doris::vectorized
